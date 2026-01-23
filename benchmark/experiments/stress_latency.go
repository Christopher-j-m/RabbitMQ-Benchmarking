// Stress Latency Experiment
//
// Measures the end-to-end latency in rmq quorum queues under heavy load conditions.
// This experiment combines the latency measurement approach from ideal_latency with
// heavy stress generation (fire-and-forget publishers) similar to linear_capacity.
//
// This simulates worst-case performance scenarios where the cluster is under heavy load
// and we measure the end-to-end latency that a single publisher/consumer pair experiences.
//
// Stress Publishers: Send messages as fast as possible without waiting for confirms (not used for measurements, just to stress the cluster)
// Stress Consumers: Consume messages as fast as possible to prevent queue backlog from influencing latency measurements (Optional)
// Latency Publisher: Send messages synchronously, waiting for ACK, inject timestamp in headers for latency measurements
// Latency Consumer: Extracts timestamp from headers to calculate end-to-end latency
// Metrics: End-to-End Latency (P99, P95, Mean)
package experiments

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net/url"
	"rmq-benchmark/metrics"
	"rmq-benchmark/rmq"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type StressLatency struct {
	config             Config
	controller         *rmq.Controller
	leaderURL          string
	leaderNodeIndex    int
	stressConnURLs     []string
	stressQueues       []string
	stressQueuesByNode [][]string
	connections        []*amqp.Connection
	payloads           [][]byte
}

func (experiment *StressLatency) Setup(config Config, controller *rmq.Controller) error {
	experiment.config = config
	experiment.controller = controller

	// Pre-generate 1000 random payloads with with the specified fixed size
	// => prevents delays in the publisher routines during the measurement
	log.Println("Generating test data...")
	experiment.payloads = GeneratePayloads(1000, config.MsgSize)
	log.Println("Data generation complete. Connecting to RabbitMQ...")

	// Connect to cluster to declare the main latency queue
	connection, err := controller.Connect(config.RabbitURL)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()

	// Delete existing main queue if it already exists
	channelDelete, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel for deletion: %w", err)
	}

	log.Printf("Deleting existing queue %s (if any)...", config.QueueName)
	_, _ = channelDelete.QueueDelete(config.QueueName, false, false, false)
	_ = channelDelete.Close()

	amqpChannel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = amqpChannel.Close() }()

	// Configure the queue as a quorum queue with the specified initial group size
	args := amqp.Table{
		"x-queue-type":                "quorum",
		"x-quorum-initial-group-size": config.QuorumSize,
	}

	// Create the main quorum queue for latency measurement
	_, err = amqpChannel.QueueDeclare(
		config.QueueName, // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		args,             // arguments
	)
	if err != nil {
		return err
	}

	// Find the leader node for the created quorum queue
	leaderNode, err := controller.GetQueueLeaderNode("/", config.QueueName)
	if err != nil {
		return err
	}
	log.Printf("Queue leader is on node: %s", leaderNode)

	// Store leader URL for worker connections
	experiment.leaderURL = fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), leaderNode)

	// Discover all cluster nodes for stress generation
	nodes, err := controller.GetNodes()
	if err != nil {
		return err
	}
	log.Printf("Discovered nodes for stress generation: %v", nodes)

	// Find the index of the leader node in the discovered nodes list
	experiment.leaderNodeIndex = -1
	for i, node := range nodes {
		if node == leaderNode {
			experiment.leaderNodeIndex = i
			break
		}
	}
	if experiment.leaderNodeIndex == -1 {
		return fmt.Errorf("leader node %s not found in cluster nodes list", leaderNode)
	}

	experiment.stressConnURLs = make([]string, len(nodes))
	experiment.stressQueues = make([]string, 0, len(nodes)*config.QueueCount)
	experiment.stressQueuesByNode = make([][]string, len(nodes))
	experiment.connections = make([]*amqp.Connection, 0)

	// Build connection URLs and create multiple stress queues on each node
	for i, node := range nodes {
		connURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), node)
		experiment.stressConnURLs[i] = connURL
		experiment.stressQueuesByNode[i] = make([]string, config.QueueCount)

		// Connect to create the stress queues
		stressConn, err := controller.Connect(connURL)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", node, err)
		}

		// Create multiple stress queues per node
		for q := 0; q < config.QueueCount; q++ {
			// Create stress queues with naming convention: <base-queue>-stress-<nodeIdx>-<queueIdx>
			queueName := fmt.Sprintf("%s-stress-%d-%d", config.QueueName, i, q)
			experiment.stressQueuesByNode[i][q] = queueName
			experiment.stressQueues = append(experiment.stressQueues, queueName)

			// Delete existing queues with the same name as the ones
			// that will be created during this experiment
			channelDelete, err := stressConn.Channel()
			if err != nil {
				_ = stressConn.Close()
				return fmt.Errorf("failed to open channel for deletion: %w", err)
			}

			log.Printf("Deleting existing stress queue %s (if any)...", queueName)
			_, _ = channelDelete.QueueDelete(queueName, false, false, false)
			_ = channelDelete.Close()

			stressChannel, err := stressConn.Channel()
			if err != nil {
				_ = stressConn.Close()
				return err
			}

			// Configure stress queue arguments, which are different from latency queue
			stressArgs := amqp.Table{
				"x-queue-type":                "quorum",
				"x-quorum-initial-group-size": config.QuorumSize,
			}

			// Add max-length and overflow settings if specified
			// This allows omitting spawning consumers by dropping messages automatically
			if config.QueueMaxLength > 0 {
				stressArgs["x-max-length"] = config.QueueMaxLength
				if config.QueueOverflowStrategy != "" {
					stressArgs["x-overflow"] = config.QueueOverflowStrategy
				}
				log.Printf("Stress queue %s: max-length=%d, overflow=%s", queueName, config.QueueMaxLength, config.QueueOverflowStrategy)
			}

			_, err = stressChannel.QueueDeclare(
				queueName,  // queue name
				true,       // durable
				false,      // delete when unused
				false,      // exclusive
				false,      // no-wait
				stressArgs, // stress-specific quorum args
			)
			_ = stressChannel.Close()
			if err != nil {
				_ = stressConn.Close()
				return err
			}
		}

		_ = stressConn.Close()
	}

	log.Printf("Created %d stress queues across %d nodes (%d queues per node)", len(experiment.stressQueues), len(nodes), config.QueueCount)
	return nil
}

func (experiment *StressLatency) Run(ctx context.Context, publishers int, metricsRecorder *metrics.Recorder) (metrics.Summary, error) {
	var waitGroup sync.WaitGroup
	var connectionMutex sync.Mutex // Protects experiment.connections slice

	// Helper to create and track a new connection
	createLeaderConnection := func() (*amqp.Connection, error) {
		connection, err := experiment.controller.Connect(experiment.leaderURL)
		if err != nil {
			return nil, err
		}
		connectionMutex.Lock()
		experiment.connections = append(experiment.connections, connection)
		connectionMutex.Unlock()
		return connection, nil
	}

	createStressConnection := func(nodeIdx int) (*amqp.Connection, error) {
		connection, err := experiment.controller.Connect(experiment.stressConnURLs[nodeIdx])
		if err != nil {
			return nil, err
		}
		connectionMutex.Lock()
		experiment.connections = append(experiment.connections, connection)
		connectionMutex.Unlock()
		return connection, nil
	}

	// CONSUMERS
	// Leader Node:
	// - Latency Consumers: Fixed at 1 consumer
	// - Stress Consumers: as specified via 'consumers' param
	// Non-leader Nodes:
	// - Stress Consumers: as specified via 'consumers' param
	if experiment.config.Consumers >= 0 {
		const latencyConsumers = 1
		stressConsumersPerNode := experiment.config.Consumers
		queueCount := len(experiment.stressQueuesByNode[0])

		log.Printf("Consumer Distribution (one connection per worker):")
		log.Printf("  - Leader Node: %d Latency + %d Stress Consumers across %d queues", latencyConsumers, stressConsumersPerNode, queueCount)
		log.Printf("  - Other Nodes: %d Stress Consumers across %d queues", stressConsumersPerNode, queueCount)

		// Start Latency Consumer (Leader only, one connection)
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			connection, err := createLeaderConnection()
			if err != nil {
				log.Printf("Latency consumer failed to create connection: %v", err)
				return
			}
			experiment.runConsumerWithConn(ctx, connection, experiment.config.QueueName, metricsRecorder)
		}()

		// Start Stress Consumers on all nodes (distributed across queues via modulo)
		for nodeIdx := range experiment.stressConnURLs {
			nodeQueues := experiment.stressQueuesByNode[nodeIdx]

			for consumerIndex := 0; consumerIndex < stressConsumersPerNode; consumerIndex++ {
				queueIdx := consumerIndex % queueCount
				queueName := nodeQueues[queueIdx]

				waitGroup.Add(1)
				go func(nodeIdx int, queueName string) {
					defer waitGroup.Done()
					connection, err := createStressConnection(nodeIdx)
					if err != nil {
						log.Printf("Stress consumer failed to create connection: %v", err)
						return
					}
					experiment.runConsumerWithConn(ctx, connection, queueName, nil)
				}(nodeIdx, queueName)
			}
		}
	} else {
		log.Println("WARNING: Consumers set to < 0, so no consumers will be started...")
	}

	// PUBLISHERS
	// Leader Node:
	// - Latency Publishers: Fixed at 1 publisher
	// - Stress Publishers: as specified via 'publishers' param
	// Non-leader Nodes:
	// - Stress Publishers: as specified via 'publishers' param
	const latencyPublishers = 1
	stressPublishersPerNode := publishers

	log.Printf("Publisher Distribution (one connection per worker):")
	log.Printf("  - Leader Node: %d Latency + %d Stress Publishers", latencyPublishers, stressPublishersPerNode)
	log.Printf("  - Other Nodes: %d Stress Publishers", stressPublishersPerNode)

	queueCount := len(experiment.stressQueuesByNode[0])
	stressCounter := 0
	for nodeIdx := range experiment.stressConnURLs {
		nodeQueues := experiment.stressQueuesByNode[nodeIdx]

		for workerIndex := 0; workerIndex < stressPublishersPerNode; workerIndex++ {
			queueIdx := workerIndex % queueCount
			queueName := nodeQueues[queueIdx]
			publisherID := stressCounter
			stressCounter++
			waitGroup.Add(1)
			go func(nodeIdx int, queueName string, publisherID int) {
				defer waitGroup.Done()
				connection, err := createStressConnection(nodeIdx)
				if err != nil {
					log.Printf("Stress publisher failed to create connection: %v", err)
					return
				}
				experiment.runStressPublisherWithConn(ctx, connection, queueName, publisherID)
			}(nodeIdx, queueName, publisherID)
		}
	}

	// Start Latency Publishers (one connection each)
	log.Printf("Starting %d latency publisher (one connection each)...", latencyPublishers)
	for i := 0; i < latencyPublishers; i++ {
		waitGroup.Add(1)
		go func(publisherID int) {
			defer waitGroup.Done()
			connection, err := createLeaderConnection()
			if err != nil {
				log.Printf("Latency publisher failed to create connection: %v", err)
				return
			}
			experiment.runLatencyPublisherWithConn(ctx, connection, publisherID, metricsRecorder)
		}(i)
	}

	// Wait for the go context to be canceled and all coroutines to exit gracefully
	waitGroup.Wait()
	return metricsRecorder.GetSummary(), nil
}

// Drain messages from a queue (connection provided by caller)
// If metricsRecorder is not nil (latency consumer), it extracts the timestamp from headers and records latency
func (experiment *StressLatency) runConsumerWithConn(ctx context.Context, connection *amqp.Connection, queueName string, metricsRecorder *metrics.Recorder) {
	amqpChannel, err := connection.Channel()
	if err != nil {
		log.Printf("Consumer failed to create channel: %v", err)
		return
	}

	// Close channel when context is done to unblock the consumer
	go func() {
		<-ctx.Done()
		_ = amqpChannel.Close()
	}()

	if err := amqpChannel.Qos(50, 0, false); err != nil {
		log.Printf("Consumer failed to set QoS: %v", err)
		return
	}

	messages, err := amqpChannel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Printf("Consumer failed to start consuming: %v", err)
		return
	}

	// Local batch for latency recording to avoid lock contention
	const batchSize = 100
	var latencyBatch []int64
	if metricsRecorder != nil {
		latencyBatch = make([]int64, 0, batchSize)
	}

	for delivery := range messages {
		// Check if context is done
		select {
		case <-ctx.Done():
			if metricsRecorder != nil && len(latencyBatch) > 0 {
				metricsRecorder.RecordLatencyBatch(latencyBatch)
			}
			return
		default:
		}

		// Extract timestamp from header and calculate end-to-end latency
		if metricsRecorder != nil {
			if sentAt, ok := delivery.Headers["x-sent-at"]; ok {
				if sentTimestamp, ok := sentAt.(int64); ok {
					latencyNs := time.Now().UnixNano() - sentTimestamp
					latencyUs := latencyNs / 1000 // Convert to microseconds
					latencyBatch = append(latencyBatch, latencyUs)
					if len(latencyBatch) >= batchSize {
						metricsRecorder.RecordLatencyBatch(latencyBatch)
						latencyBatch = latencyBatch[:0]
					}
				}
			}
		}

		_ = delivery.Ack(false)
	}

	// Flush remaining latencies when channel closes
	if metricsRecorder != nil && len(latencyBatch) > 0 {
		metricsRecorder.RecordLatencyBatch(latencyBatch)
	}
}

// Fire messages as fast as possible without waiting for confirms (connection provided by caller)
func (experiment *StressLatency) runStressPublisherWithConn(ctx context.Context, connection *amqp.Connection, queueName string, publisherID int) {
	randomGenerator := rand.New(rand.NewPCG(uint64(publisherID+1000), uint64(time.Now().UnixNano())))

	amqpChannel, err := connection.Channel()
	if err != nil {
		log.Printf("Stress publisher failed to create channel: %v", err)
		return
	}
	defer func() { _ = amqpChannel.Close() }()

	if err := amqpChannel.Confirm(false); err != nil {
		log.Printf("Stress publisher failed to enable confirms: %v", err)
		return
	}

	// Pipelined publishing: buffered channel allows 2000 messages in-flight
	confirmations := amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 2000))

	// Background routine to drain confirms (we don't care about latency for stress)
	go func() {
		for range confirmations {
			// Drain confirms without recording
		}
	}()

	// Non-blocking publish loop
	for {
		select {
		case <-ctx.Done():
			return
		default:
			payload := experiment.payloads[randomGenerator.IntN(len(experiment.payloads))]

			err := amqpChannel.PublishWithContext(ctx,
				"",        // exchange
				queueName, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        payload,
				},
			)
			if err != nil {
				if err == amqp.ErrClosed {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// Send messages synchronously, waiting for ACK & with timestamp header (connection provided by caller)
func (experiment *StressLatency) runLatencyPublisherWithConn(ctx context.Context, connection *amqp.Connection, publisherID int, metricsRecorder *metrics.Recorder) {
	randomGenerator := rand.New(rand.NewPCG(uint64(publisherID), uint64(time.Now().UnixNano())))

	amqpChannel, err := connection.Channel()
	if err != nil {
		log.Printf("Latency publisher failed to create channel: %v", err)
		return
	}
	defer func() { _ = amqpChannel.Close() }()

	if err := amqpChannel.Confirm(false); err != nil {
		log.Printf("Latency publisher failed to enable confirms: %v", err)
		return
	}

	confirmations := amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			payload := experiment.payloads[randomGenerator.IntN(len(experiment.payloads))]

			// Publish with timestamp header for end-to-end latency calculation
			err := amqpChannel.PublishWithContext(ctx,
				"",                          // exchange
				experiment.config.QueueName, // routing key (main latency queue)
				false,                       // mandatory
				false,                       // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Headers:     amqp.Table{"x-sent-at": time.Now().UnixNano()},
					Body:        payload,
				},
			)

			if err != nil {
				if err == amqp.ErrClosed {
					return
				}
				metricsRecorder.RecordError()
				time.Sleep(50 * time.Millisecond)
				continue
			}

			// Wait for ACK
			select {
			case <-ctx.Done():
				return
			case confirmation := <-confirmations:
				if !confirmation.Ack {
					metricsRecorder.RecordError()
				}

			case <-time.After(30 * time.Second):
				log.Printf("Latency publisher timed out waiting for confirm")
				metricsRecorder.RecordError()
				time.Sleep(time.Duration(1000+randomGenerator.IntN(2000)) * time.Millisecond)
			}
		}
	}
}

func (experiment *StressLatency) Teardown() error {
	for _, connection := range experiment.connections {
		if connection != nil {
			_ = connection.Close()
		}
	}
	return nil
}
