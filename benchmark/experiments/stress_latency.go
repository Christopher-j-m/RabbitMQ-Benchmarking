// Stress Latency Experiment
//
// Measures the end-to-end latency in rmq quorum queues under heavy load conditions.
// This experiment combines the latency measurement approach from ideal_latency with
// heavy stress generation (fire-and-forget publishers) similar to linear_capacity.
//
// This simulates worst-case performance scenarios where the cluster is under
// heavy load and we want to measure the end-to-end latency experienced.
//
// Stress Publishers: Send messages as fast as possible without waiting for confirms (not used for measurements, just to stress the cluster)
// Latency Publishers: Send messages synchronously, waiting for ACK, inject timestamp in headers (used for latency measurements)
// Consumers: Extract timestamp from headers to calculate end-to-end latency
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

	"github.com/go-faker/faker/v4"
	amqp "github.com/rabbitmq/amqp091-go"
)

type StressLatency struct {
	config             Config
	ctrl               *rmq.Controller
	leaderURL          string
	leaderNodeIndex    int
	stressConnURLs     []string
	stressQueues       []string
	stressQueuesByNode [][]string
	conns              []*amqp.Connection
	payloads           [][]byte
}

func (e *StressLatency) Setup(config Config, ctrl *rmq.Controller) error {
	e.config = config
	e.ctrl = ctrl

	// Pre-generate 1000 payloads with random sizes (1KB to 5KB)
	// => prevents delays in the publisher routines during the measurement
	log.Println("Generating test data...")
	e.payloads = make([][]byte, 1000)
	for i := range e.payloads {
		size := 1024 + rand.IntN(4096)
		text := faker.Paragraph()
		b := make([]byte, size)
		copy(b, text)
		if len(text) < size {
			for j := len(text); j < size; j++ {
				b[j] = byte(rand.IntN(256))
			}
		}
		e.payloads[i] = b
	}
	log.Println("Data generation complete. Connecting to RabbitMQ...")

	// Connect to cluster to declare the main latency queue
	conn, err := ctrl.Connect(config.RabbitURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Delete existing main queue if it already exists
	deleteCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel for deletion: %w", err)
	}

	log.Printf("Deleting existing queue %s (if any)...", config.QueueName)
	_, _ = deleteCh.QueueDelete(config.QueueName, false, false, false)
	deleteCh.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Configure the queue as a quorum queue with the specified initial group size
	args := amqp.Table{
		"x-queue-type":                "quorum",
		"x-quorum-initial-group-size": config.QuorumSize,
	}

	// Create the main quorum queue for latency measurement
	_, err = ch.QueueDeclare(
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
	leaderNode, err := ctrl.GetQueueLeaderNode("/", config.QueueName)
	if err != nil {
		return err
	}
	log.Printf("Queue leader is on node: %s", leaderNode)

	// Store leader URL for worker connections
	e.leaderURL = fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), leaderNode)

	// Discover all cluster nodes for stress generation
	nodes, err := ctrl.GetNodes()
	if err != nil {
		return err
	}
	log.Printf("Discovered nodes for stress generation: %v", nodes)

	// Find the index of the leader node in the discovered nodes list
	e.leaderNodeIndex = -1
	for i, node := range nodes {
		if node == leaderNode {
			e.leaderNodeIndex = i
			break
		}
	}
	if e.leaderNodeIndex == -1 {
		return fmt.Errorf("leader node %s not found in cluster nodes list", leaderNode)
	}

	e.stressConnURLs = make([]string, len(nodes))
	e.stressQueues = make([]string, 0, len(nodes)*config.QueueCount)
	e.stressQueuesByNode = make([][]string, len(nodes))
	e.conns = make([]*amqp.Connection, 0)

	// Build connection URLs and create multiple stress queues on each node
	for i, node := range nodes {
		connURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), node)
		e.stressConnURLs[i] = connURL
		e.stressQueuesByNode[i] = make([]string, config.QueueCount)

		// Connect to create the stress queues
		stressConn, err := ctrl.Connect(connURL)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", node, err)
		}

		// Create multiple stress queues per node
		for q := 0; q < config.QueueCount; q++ {
			// Create stress queues with naming convention: <base-queue>-stress-<nodeIdx>-<queueIdx>
			qName := fmt.Sprintf("%s-stress-%d-%d", config.QueueName, i, q)
			e.stressQueuesByNode[i][q] = qName
			e.stressQueues = append(e.stressQueues, qName)

			// Delete existing queues with the same name as the ones
			// that will be created during this experiment
			deleteCh, err := stressConn.Channel()
			if err != nil {
				stressConn.Close()
				return fmt.Errorf("failed to open channel for deletion: %w", err)
			}

			log.Printf("Deleting existing stress queue %s (if any)...", qName)
			_, _ = deleteCh.QueueDelete(qName, false, false, false)
			deleteCh.Close()

			stressCh, err := stressConn.Channel()
			if err != nil {
				stressConn.Close()
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
				log.Printf("Stress queue %s: max-length=%d, overflow=%s", qName, config.QueueMaxLength, config.QueueOverflowStrategy)
			}

			_, err = stressCh.QueueDeclare(
				qName,      // queue name
				true,       // durable
				false,      // delete when unused
				false,      // exclusive
				false,      // no-wait
				stressArgs, // stress-specific quorum args
			)
			stressCh.Close()
			if err != nil {
				stressConn.Close()
				return err
			}
		}

		stressConn.Close()
	}

	log.Printf("Created %d stress queues across %d nodes (%d queues per node)", len(e.stressQueues), len(nodes), config.QueueCount)
	return nil
}

func (e *StressLatency) Run(ctx context.Context, publishers int, rec *metrics.Recorder) (metrics.Summary, error) {
	var wg sync.WaitGroup
	var connMu sync.Mutex // Protects e.conns slice

	// Helper to create and track a new connection
	createLeaderConnection := func() (*amqp.Connection, error) {
		conn, err := e.ctrl.Connect(e.leaderURL)
		if err != nil {
			return nil, err
		}
		connMu.Lock()
		e.conns = append(e.conns, conn)
		connMu.Unlock()
		return conn, nil
	}

	createStressConnection := func(nodeIdx int) (*amqp.Connection, error) {
		conn, err := e.ctrl.Connect(e.stressConnURLs[nodeIdx])
		if err != nil {
			return nil, err
		}
		connMu.Lock()
		e.conns = append(e.conns, conn)
		connMu.Unlock()
		return conn, nil
	}

	// CONSUMERS
	// Leader Node:
	// - Latency Consumers: Fixed at 1 consumer
	// - Stress Consumers: as specified via 'consumers' param
	// Non-leader Nodes:
	// - Stress Consumers: as specified via 'consumers' param
	if e.config.Consumers >= 0 {
		const latencyConsumers = 1
		stressConsumersPerNode := e.config.Consumers
		queueCount := len(e.stressQueuesByNode[0])

		log.Printf("Consumer Distribution (one connection per worker):")
		log.Printf("  - Leader Node: %d Latency + %d Stress Consumers across %d queues", latencyConsumers, stressConsumersPerNode, queueCount)
		log.Printf("  - Other Nodes: %d Stress Consumers across %d queues", stressConsumersPerNode, queueCount)

		// Start Latency Consumer (Leader only, one connection)
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := createLeaderConnection()
			if err != nil {
				log.Printf("Latency consumer failed to create connection: %v", err)
				return
			}
			e.runConsumerWithConn(ctx, conn, e.config.QueueName, rec)
		}()

		// Start Stress Consumers on all nodes (distributed across queues via modulo)
		for nodeIdx := range e.stressConnURLs {
			nodeQueues := e.stressQueuesByNode[nodeIdx]

			for c := 0; c < stressConsumersPerNode; c++ {
				queueIdx := c % queueCount
				queueName := nodeQueues[queueIdx]

				wg.Add(1)
				go func(nodeIdx int, q string) {
					defer wg.Done()
					conn, err := createStressConnection(nodeIdx)
					if err != nil {
						log.Printf("Stress consumer failed to create connection: %v", err)
						return
					}
					e.runConsumerWithConn(ctx, conn, q, nil)
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
	
	queueCount := len(e.stressQueuesByNode[0])
	stressCounter := 0
	for nodeIdx := range e.stressConnURLs {
		nodeQueues := e.stressQueuesByNode[nodeIdx]

		for w := 0; w < stressPublishersPerNode; w++ {
			queueIdx := w % queueCount
			queueName := nodeQueues[queueIdx]
			publisherID := stressCounter
			stressCounter++
			wg.Add(1)
			go func(nodeIdx int, q string, pid int) {
				defer wg.Done()
				conn, err := createStressConnection(nodeIdx)
				if err != nil {
					log.Printf("Stress publisher failed to create connection: %v", err)
					return
				}
				e.runStressPublisherWithConn(ctx, conn, q, pid)
			}(nodeIdx, queueName, publisherID)
		}
	}

	// Start Latency Publishers (one connection each)
	log.Printf("Starting %d latency publisher (one connection each)...", latencyPublishers)
	for i := 0; i < latencyPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
			conn, err := createLeaderConnection()
			if err != nil {
				log.Printf("Latency publisher failed to create connection: %v", err)
				return
			}
			e.runLatencyPublisherWithConn(ctx, conn, publisherID, rec)
		}(i)
	}

	// Wait for the go context to be canceled and all coroutines to exit gracefully
	wg.Wait()
	return rec.GetSummary(), nil
}

// Drain messages from a queue (connection provided by caller)
// If rec is not nil (latency consumer), it extracts the timestamp from headers and records latency
func (e *StressLatency) runConsumerWithConn(ctx context.Context, conn *amqp.Connection, queueName string, rec *metrics.Recorder) {
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Consumer failed to create channel: %v", err)
		return
	}

	// Close channel when context is done to unblock the consumer
	go func() {
		<-ctx.Done()
		ch.Close()
	}()

	if err := ch.Qos(50, 0, false); err != nil {
		log.Printf("Consumer failed to set QoS: %v", err)
		return
	}

	msgs, err := ch.Consume(
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
	if rec != nil {
		latencyBatch = make([]int64, 0, batchSize)
	}

	for d := range msgs {
		// Check if context is done
		select {
		case <-ctx.Done():
			if rec != nil && len(latencyBatch) > 0 {
				rec.RecordLatencyBatch(latencyBatch)
			}
			return
		default:
		}

		// Extract timestamp from header and calculate end-to-end latency
		if rec != nil {
			if sentAt, ok := d.Headers["x-sent-at"]; ok {
				if sentTimestamp, ok := sentAt.(int64); ok {
					latencyNs := time.Now().UnixNano() - sentTimestamp
					latencyUs := latencyNs / 1000 // Convert to microseconds
					latencyBatch = append(latencyBatch, latencyUs)
					if len(latencyBatch) >= batchSize {
						rec.RecordLatencyBatch(latencyBatch)
						latencyBatch = latencyBatch[:0]
					}
				}
			}
		}

		d.Ack(false)
	}

	// Flush remaining latencies when channel closes
	if rec != nil && len(latencyBatch) > 0 {
		rec.RecordLatencyBatch(latencyBatch)
	}
}

// Fire messages as fast as possible without waiting for confirms (connection provided by caller)
func (e *StressLatency) runStressPublisherWithConn(ctx context.Context, conn *amqp.Connection, queueName string, publisherID int) {
	rng := rand.New(rand.NewPCG(uint64(publisherID+1000), uint64(time.Now().UnixNano())))

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Stress publisher failed to create channel: %v", err)
		return
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		log.Printf("Stress publisher failed to enable confirms: %v", err)
		return
	}

	// Pipelined publishing: buffered channel allows 2000 messages in-flight
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 2000))

	// Background routine to drain confirms (we don't care about latency for stress)
	go func() {
		for range confirms {
			// Drain confirms without recording
		}
	}()

	// Non-blocking publish loop
	for {
		select {
		case <-ctx.Done():
			return
		default:
			payload := e.payloads[rng.IntN(len(e.payloads))]

			err := ch.PublishWithContext(ctx,
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
func (e *StressLatency) runLatencyPublisherWithConn(ctx context.Context, conn *amqp.Connection, publisherID int, rec *metrics.Recorder) {
	rng := rand.New(rand.NewPCG(uint64(publisherID), uint64(time.Now().UnixNano())))

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Latency publisher failed to create channel: %v", err)
		return
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		log.Printf("Latency publisher failed to enable confirms: %v", err)
		return
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			payload := e.payloads[rng.IntN(len(e.payloads))]

			// Publish with timestamp header for end-to-end latency calculation
			err := ch.PublishWithContext(ctx,
				"",                 // exchange
				e.config.QueueName, // routing key (main latency queue)
				false,              // mandatory
				false,              // immediate
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
				rec.RecordError()
				time.Sleep(50 * time.Millisecond)
				continue
			}

			// Wait for ACK
			select {
			case <-ctx.Done():
				return
			case conf := <-confirms:
				if !conf.Ack {
					rec.RecordError()
				}

			case <-time.After(30 * time.Second):
				log.Printf("Latency publisher timed out waiting for confirm")
				rec.RecordError()
				time.Sleep(time.Duration(1000+rng.IntN(2000)) * time.Millisecond)
			}
		}
	}
}

func (e *StressLatency) Teardown() error {
	for _, conn := range e.conns {
		if conn != nil {
			conn.Close()
		}
	}
	return nil
}
