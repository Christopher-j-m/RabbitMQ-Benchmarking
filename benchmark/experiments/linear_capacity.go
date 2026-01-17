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
	ctrl         *rmq.Controller
	nodes        []string
	connURLs     []string
	conns        []*amqp.Connection
	queues       []string
	queuesByNode [][]string
	payloads     [][]byte
}

func (e *LinearCapacity) Setup(config Config, ctrl *rmq.Controller) error {
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

	// Discover all cluster nodes
	nodes, err := ctrl.GetNodes()
	if err != nil {
		return err
	}
	log.Printf("Discovered nodes: %v", nodes)

	e.nodes = nodes
	e.connURLs = make([]string, len(nodes))
	e.queues = make([]string, 0, len(nodes)*config.QueueCount)
	e.queuesByNode = make([][]string, len(nodes))
	e.conns = make([]*amqp.Connection, 0)

	// Connect to each discovered node and create multiple quorum queues per node
	for i, node := range nodes {
		// Build the connection string for the curr node
		connURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), node)
		e.connURLs[i] = connURL
		e.queuesByNode[i] = make([]string, config.QueueCount)

		// Connect to create the queues
		conn, err := ctrl.Connect(connURL)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", node, err)
		}

		// Create multiple queues per node
		for q := 0; q < config.QueueCount; q++ {
			// Set the queue name based on the given base name, node index, and sub-queue index
			// e.g. queue "test-queue" becomes "test-queue-0-0", "test-queue-0-1", ... on node 0
			qName := fmt.Sprintf("%s-%d-%d", config.QueueName, i, q)
			e.queuesByNode[i][q] = qName
			e.queues = append(e.queues, qName)

			// Delete existing queues with the same name as the ones
			// that will be created during this experiment
			deleteCh, err := conn.Channel()
			if err != nil {
				conn.Close()
				return fmt.Errorf("failed to create delete channel: %w", err)
			}

			log.Printf("Deleting existing queue %s...", qName)
			_, _ = deleteCh.QueueDelete(qName, false, false, false)
			deleteCh.Close()

			// Create a channel for queue declaration
			ch, err := conn.Channel()
			if err != nil {
				conn.Close()
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
				log.Printf("Queue %s: queue-length=%d, queue-overflow=%s", qName, config.QueueMaxLength, config.QueueOverflowStrategy)
			}

			// Create the quorum queue
			_, err = ch.QueueDeclare(
				qName, // queue name
				true,  // durable - survives broker restarts
				false, // delete when unused
				false, // exclusive - not limited to this connection
				false, // no-wait - wait for rmq ack that queue is created
				args,  // optional args
			)

			ch.Close()
			if err != nil {
				conn.Close()
				return err
			}
		}

		conn.Close()
	}

	log.Printf("Created %d queues across %d nodes (%d queues per node)", len(e.queues), len(nodes), config.QueueCount)
	return nil
}

func (e *LinearCapacity) Run(ctx context.Context, publishers int, rec *metrics.Recorder) (metrics.Summary, error) {
	var wg sync.WaitGroup
	var connMu sync.Mutex

	// Helper to create and track a new connection
	createConnection := func(nodeIdx int) (*amqp.Connection, error) {
		conn, err := e.ctrl.Connect(e.connURLs[nodeIdx])
		if err != nil {
			return nil, err
		}
		connMu.Lock()
		e.conns = append(e.conns, conn)
		connMu.Unlock()

		// Listen for amqp.Blocking alarms on the connection with the leader node.
		// These alarms (memory/disk pressure) are logged to detect conditions that
		// invalidate the measurements
		// => https://www.rabbitmq.com/docs/connection-blocked
		nodeName := fmt.Sprintf("Node-%d-Conn-%d", nodeIdx, len(e.conns))
		blockings := conn.NotifyBlocked(make(chan amqp.Blocking))
		go func(name string, c <-chan amqp.Blocking) {
			var blockStart time.Time
			for b := range c {
				if b.Active {
					blockStart = time.Now()
					log.Printf("[BLOCKED] %s - Reason: %q", name, b.Reason)
				} else {
					duration := time.Since(blockStart)
					log.Printf("[UNBLOCKED] %s - Duration: %v", name, duration)
				}
			}
		}(nodeName, blockings)

		return conn, nil
	}

	// Start the specified number of consumers on each queue
	// Consumers are distributed mostly evenly (modulo) across all queues on each node
	if e.config.Consumers > 0 {
		consumersPerNode := e.config.Consumers
		queueCount := len(e.queuesByNode[0])
		totalConsumers := consumersPerNode * len(e.nodes)

		log.Printf("Starting %d consumers per node distributed across %d queues (Total: %d)...", consumersPerNode, queueCount, totalConsumers)

		for nodeIdx := range e.nodes {
			queuesByNode := e.queuesByNode[nodeIdx]

			for c := 0; c < consumersPerNode; c++ {
				queueIdx := c % queueCount
				queueName := queuesByNode[queueIdx]

				wg.Add(1)
				go func(nodeIdx int, q string) {
					defer wg.Done()

					conn, err := createConnection(nodeIdx)
					if err != nil {
						log.Printf("Consumer failed to create connection: %v", err)
						return
					}

					ch, err := conn.Channel()
					if err != nil {
						log.Printf("Consumer failed to create channel: %v", err)
						return
					}
					defer ch.Close()

					// Set QoS for the consumer channel to prefetch 50 messages
					// Consumers will receive up to 50 unacknowledged messages at a time
					// => Prevents flooding the consumer with too many messages
					if err := ch.Qos(50, 0, false); err != nil {
						log.Printf("Consumer failed to set QoS: %v", err)
						return
					}

					// Start consuming messages
					// => pkg.go.dev/github.com/rabbitmq/amqp091-go@v1.10.0#Channel.Consume
					msgs, err := ch.Consume(
						q,     // queue
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
						case d, ok := <-msgs:
							if !ok {
								return
							}
							d.Ack(false)
						}
					}
				}(nodeIdx, queueName)
			}
		}
	}

	// Start the specified number of publishers on each node
	// Publishers are distributed mostly evenly (modulo) across all queues on each node
	publishersPerNode := publishers
	queueCount := len(e.queuesByNode[0])
	totalPublishers := publishers * len(e.nodes)

	log.Printf("Starting %d publishers per node distributed across %d queues (Total: %d)...", publishersPerNode, queueCount, totalPublishers)

	publisherCounter := 0
	for nodeIdx := range e.nodes {
		queuesByNode := e.queuesByNode[nodeIdx]

		for w := 0; w < publishersPerNode; w++ {
			queueIdx := w % queueCount
			queueName := queuesByNode[queueIdx]
			publisherID := publisherCounter
			publisherCounter++
			wg.Add(1)
			go func(nodeIdx int, q string, pid int) {
				defer wg.Done()

				// Create dedicated connection for this publisher
				conn, err := createConnection(nodeIdx)
				if err != nil {
					log.Printf("Publisher failed to create connection: %v", err)
					return
				}

				// Avoid global mutex contention on rand by using own RNG per publisher
				// => https://go.dev/blog/randv2
				rng := rand.New(rand.NewPCG(uint64(pid), uint64(time.Now().UnixNano())))

				ch, err := conn.Channel()
				if err != nil {
					log.Printf("Publisher failed to create channel: %v", err)
					return
				}
				defer ch.Close()

				if err := ch.Confirm(false); err != nil {
					log.Printf("Publisher failed to enable confirms: %v", err)
					return
				}

				// Pipelined publishing: buffered channel allows 2000 messages in-flight
				// Allows publishers to continue sending without waiting for acks from rmq (acks handled asynchronously)
				// => maximizes throughput
				confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 2000))

				// Background routing to handle the async acks from rmq
				// Records in batches to reduce mutex contention in the recorder, since we expect high throughput here
				// => Locking a mutex for every single message would cause contention and therefore reduce throughput of load generator
				go func() {
					const batchSize = 100
					latencyBatch := make([]int64, 0, batchSize)
					for conf := range confirms {
						if conf.Ack {
							// For capacity tests, we measure throughput not individual latency
							latencyBatch = append(latencyBatch, 0)
							if len(latencyBatch) >= batchSize {
								rec.RecordLatencyBatch(latencyBatch)
								latencyBatch = latencyBatch[:0]
							}
						} else {
							rec.RecordError()
						}
					}
					// Flush remaining
					if len(latencyBatch) > 0 {
						rec.RecordLatencyBatch(latencyBatch)
					}
				}()

				// Non-blocking publish loop
				for {
					select {
					case <-ctx.Done():
						return
					default:
						// Select a random, pre-generated payload that we created during setup
						payload := e.payloads[rng.IntN(len(e.payloads))]

						// Publish the message
						err := ch.PublishWithContext(ctx,
							"",    // exchange - default: route to queue with name equal to routing key
							q,     // routing key (queue name)
							false, // mandatory - unroutable messages are dropped
							false, // immediate - no immediate delivery required
							amqp.Publishing{
								ContentType: "text/plain",
								Body:        payload,
							},
						)
						if err != nil {
							rec.RecordError()
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
	wg.Wait()
	return rec.GetSummary(), nil
}

func (e *LinearCapacity) Teardown() error {
	for _, conn := range e.conns {
		conn.Close()
	}
	return nil
}
