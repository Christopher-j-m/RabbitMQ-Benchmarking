// Linear Capacity Experiment
//
// Measures the throughput of a RabbitMQ cluster. To bypass the single-threaded bottleneck
// of individual Erlang queue processes, this experiment supports creating multiple queues per node in order to allow 
// maximizing throughput.
//
// Publishers: Send messages as fast as possible without waiting for confirms
// Consumers: Consume messages as fast as possible and ack them in order to free up memory and avoid blocks on the nodes
// Metrics: Throughput (msgs/sec)
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

type LinearCapacity struct {
	config       Config
	controller   *rmq.Controller
	nodes        []string
	connURLs     []string
	connections  []*amqp.Connection
	queues       []string
	queuesByNode [][]string
	payloads     [][]byte
}

func (experiment *LinearCapacity) Setup(config Config, controller *rmq.Controller) error {
	experiment.config = config
	experiment.controller = controller

	// Pre-generate 1000 payloads with random sizes (1KB to 5KB)
	// => prevents delays in the publisher routines during the measurement
	log.Println("Generating test data...")
	experiment.payloads = make([][]byte, 1000)
	for i := range experiment.payloads {
		size := 1024 + rand.IntN(4096)
		text := faker.Paragraph()
		b := make([]byte, size)
		copy(b, text)
		if len(text) < size {
			for j := len(text); j < size; j++ {
				b[j] = byte(rand.IntN(256))
			}
		}
		experiment.payloads[i] = b
	}
	log.Println("Data generation complete. Connecting to RabbitMQ...")

	// Discover all cluster nodes
	nodes, err := controller.GetNodes()
	if err != nil {
		return err
	}
	log.Printf("Discovered nodes: %v", nodes)

	experiment.nodes = nodes
	experiment.connURLs = make([]string, len(nodes))
	experiment.queues = make([]string, 0, len(nodes)*config.QueueCount)
	experiment.queuesByNode = make([][]string, len(nodes))
	experiment.connections = make([]*amqp.Connection, 0)

	// Connect to each discovered node and create multiple quorum queues per node
	for i, node := range nodes {
		// Build the connection string for the curr node
		connURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), node)
		experiment.connURLs[i] = connURL
		experiment.queuesByNode[i] = make([]string, config.QueueCount)

		// Connect to create the queues
		connection, err := controller.Connect(connURL)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", node, err)
		}

		// Create multiple queues per node
		for q := 0; q < config.QueueCount; q++ {
			// Set the queue name based on the given base name, node index, and sub-queue index
			// e.g. queue "test-queue" becomes "test-queue-0-0", "test-queue-0-1", ... on node 0
			queueName := fmt.Sprintf("%s-%d-%d", config.QueueName, i, q)
			experiment.queuesByNode[i][q] = queueName
			experiment.queues = append(experiment.queues, queueName)

			// Delete existing queues with the same name as the ones
			// that will be created during this experiment
			channelDelete, err := connection.Channel()
			if err != nil {
				connection.Close()
				return fmt.Errorf("failed to create delete channel: %w", err)
			}

			log.Printf("Deleting existing queue %s...", queueName)
			_, _ = channelDelete.QueueDelete(queueName, false, false, false)
			channelDelete.Close()

			// Create a channel for queue declaration
			amqpChannel, err := connection.Channel()
			if err != nil {
				connection.Close()
				return err
			}

			// Configure the queue as a quorum queue with the replication group size.
			args := amqp.Table{
				"x-queue-type":                "quorum",
				"x-quorum-initial-group-size": config.QuorumSize,
			}

			// Add max-length and overflow settings if specified
			// This allows running throughput tests without consumers by dropping messages automatically
			// => This observably reduced the cpu usage on the cluster nodes, which in turn allows higher throughput in many cases
			if config.QueueMaxLength > 0 {
				args["x-max-length"] = config.QueueMaxLength
				if config.QueueOverflowStrategy != "" {
					args["x-overflow"] = config.QueueOverflowStrategy
				}
				log.Printf("Queue %s: queue-length=%d, queue-overflow=%s", queueName, config.QueueMaxLength, config.QueueOverflowStrategy)
			}

			// Create the quorum queue
			_, err = amqpChannel.QueueDeclare(
				queueName, // queue name
				true,  // durable - survives broker restarts
				false, // delete when unused
				false, // exclusive - not limited to this connection
				false, // no-wait - wait for rmq ack that queue is created
				args,  // optional args
			)

			amqpChannel.Close()
			if err != nil {
				connection.Close()
				return err
			}
		}

		connection.Close()
	}

	log.Printf("Created %d queues across %d nodes (%d queues per node)", len(experiment.queues), len(nodes), config.QueueCount)
	return nil
}

func (experiment *LinearCapacity) Run(ctx context.Context, publishers int, metricsRecorder *metrics.Recorder) (metrics.Summary, error) {
	var waitGroup sync.WaitGroup
	var connectionMutex sync.Mutex

	// Helper to create and track a new connection
	createConnection := func(nodeIdx int) (*amqp.Connection, error) {
		connection, err := experiment.controller.Connect(experiment.connURLs[nodeIdx])
		if err != nil {
			return nil, err
		}
		connectionMutex.Lock()
		experiment.connections = append(experiment.connections, connection)
		connectionMutex.Unlock()

		// Listen for amqp.Blocking alarms on the connection with the leader node.
		// These alarms (memory/disk pressure) are logged to detect conditions that
		// invalidate the measurements
		// => https://www.rabbitmq.com/docs/connection-blocked
		nodeName := fmt.Sprintf("Node-%d-Conn-%d", nodeIdx, len(experiment.connections))
		blockings := connection.NotifyBlocked(make(chan amqp.Blocking))
		go func(name string, c <-chan amqp.Blocking) {
			var blockStart time.Time
			for blocking := range c {
				if blocking.Active {
					blockStart = time.Now()
					log.Printf("[BLOCKED] %s - Reason: %q", name, blocking.Reason)
				} else {
					duration := time.Since(blockStart)
					log.Printf("[UNBLOCKED] %s - Duration: %v", name, duration)
				}
			}
		}(nodeName, blockings)

		return connection, nil
	}

	// Start the specified number of consumers on each queue
	// Consumers are distributed mostly evenly (modulo) across all queues on each node
	if experiment.config.Consumers > 0 {
		consumersPerNode := experiment.config.Consumers
		queueCount := len(experiment.queuesByNode[0])
		totalConsumers := consumersPerNode * len(experiment.nodes)

		log.Printf("Starting %d consumers per node distributed across %d queues (Total: %d)...", consumersPerNode, queueCount, totalConsumers)

		for nodeIdx := range experiment.nodes {
			queuesByNode := experiment.queuesByNode[nodeIdx]

			for consumerIndex := 0; consumerIndex < consumersPerNode; consumerIndex++ {
				queueIdx := consumerIndex % queueCount
				queueName := queuesByNode[queueIdx]

				waitGroup.Add(1)
				go func(nodeIdx int, queueName string) {
					defer waitGroup.Done()

					connection, err := createConnection(nodeIdx)
					if err != nil {
						log.Printf("Consumer failed to create connection: %v", err)
						return
					}

					amqpChannel, err := connection.Channel()
					if err != nil {
						log.Printf("Consumer failed to create channel: %v", err)
						return
					}
					defer amqpChannel.Close()

					// Set QoS for the consumer channel to prefetch 50 messages
					// Consumers will receive up to 50 unacknowledged messages at a time
					// => Prevents flooding the consumer with too many messages
					if err := amqpChannel.Qos(50, 0, false); err != nil {
						log.Printf("Consumer failed to set QoS: %v", err)
						return
					}

					// Start consuming messages
					// => pkg.go.dev/github.com/rabbitmq/amqp091-go@v1.10.0#Channel.Consume
					messages, err := amqpChannel.Consume(
						queueName,     // queue
						"",    // consumer - unique consumer identifier
						false, // auto-ack - consumer must ack msgs
						false, // exclusive - not the only consumer for this queue
						false, // no-local - allow consuming msgs from the same connection
						false, // no-wait - wait for rmq confirmation that consumer is registered
						nil,   // specific args for queue or node server
					)
					if err != nil {
						log.Printf("Consumer failed to start consuming: %v", err)
						return
					}

					// Acknowledge messages as they arrive
					for {
						select {
						case <-ctx.Done():
							return
						case delivery, ok := <-messages:
							if !ok {
								return
							}
							delivery.Ack(false)
						}
					}
				}(nodeIdx, queueName)
			}
		}
	}

	// Start the specified number of publishers on each node
	// Publishers are distributed mostly evenly (modulo) across all queues on each node
	publishersPerNode := publishers
	queueCount := len(experiment.queuesByNode[0])
	totalPublishers := publishers * len(experiment.nodes)

	log.Printf("Starting %d publishers per node distributed across %d queues (Total: %d)...", publishersPerNode, queueCount, totalPublishers)

	publisherCounter := 0
	for nodeIdx := range experiment.nodes {
		queuesByNode := experiment.queuesByNode[nodeIdx]

		for publisherIndex := 0; publisherIndex < publishersPerNode; publisherIndex++ {
			queueIdx := publisherIndex % queueCount
			queueName := queuesByNode[queueIdx]
			publisherID := publisherCounter
			publisherCounter++
			waitGroup.Add(1)
			go func(nodeIdx int, queueName string, publisherID int) {
				defer waitGroup.Done()

				// Create dedicated connection for this publisher
				connection, err := createConnection(nodeIdx)
				if err != nil {
					log.Printf("Publisher failed to create connection: %v", err)
					return
				}

				// Avoid global mutex contention on rand by using own RNG per publisher
				// => https://go.dev/blog/randv2
				randomGenerator := rand.New(rand.NewPCG(uint64(publisherID), uint64(time.Now().UnixNano())))

				amqpChannel, err := connection.Channel()
				if err != nil {
					log.Printf("Publisher failed to create channel: %v", err)
					return
				}
				defer amqpChannel.Close()

				if err := amqpChannel.Confirm(false); err != nil {
					log.Printf("Publisher failed to enable confirms: %v", err)
					return
				}

				// Pipelined publishing: buffered channel allows 2000 messages in-flight
				// Allows publishers to continue sending without waiting for acks from rmq (acks handled asynchronously)
				// => maximizes throughput
				confirmations := amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 2000))

				// Background routing to handle the async acks from rmq
				// Records in batches to reduce mutex contention in the recorder, since we expect high throughput here
				// => Locking a mutex for every single message would cause contention and therefore reduce throughput of load generator
				go func() {
					const batchSize = 100
					latencyBatch := make([]int64, 0, batchSize)
					for confirmation := range confirmations {
						if confirmation.Ack {
							// For capacity tests, we measure throughput not individual latency
							latencyBatch = append(latencyBatch, 0)
							if len(latencyBatch) >= batchSize {
								metricsRecorder.RecordLatencyBatch(latencyBatch)
								latencyBatch = latencyBatch[:0]
							}
						} else {
							metricsRecorder.RecordError()
						}
					}
					// Flush remaining
					if len(latencyBatch) > 0 {
						metricsRecorder.RecordLatencyBatch(latencyBatch)
					}
				}()

				// Non-blocking publish loop
				for {
					select {
					case <-ctx.Done():
						return
					default:
						// Select a random, pre-generated payload that we created during setup
						payload := experiment.payloads[randomGenerator.IntN(len(experiment.payloads))]

						// Publish the message
						err := amqpChannel.PublishWithContext(ctx,
							"",    // exchange - default: route to queue with name equal to routing key
							queueName,     // routing key (queue name)
							false, // mandatory - unroutable messages are dropped
							false, // immediate - no immediate delivery required
							amqp.Publishing{
								ContentType: "text/plain",
								Body:        payload,
							},
						)
						if err != nil {
							metricsRecorder.RecordError()
							// Pause on error to prevent a tight CPU loop if the
							// network or broker is completely unavailable.
							time.Sleep(10 * time.Millisecond)
						}
					}
				}
			}(nodeIdx, queueName, publisherID)
		}
	}

	// Wait for the go context to be canceled and all coroutines to exit gracefully
	waitGroup.Wait()
	return metricsRecorder.GetSummary(), nil
}

func (experiment *LinearCapacity) Teardown() error {
	for _, connection := range experiment.connections {
		connection.Close()
	}
	return nil
}