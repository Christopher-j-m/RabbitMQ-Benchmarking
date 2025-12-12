// Linear Capacity Experiment
//
// Measures the maximum sustainable throughput of an rmq cluster with a given number of publishers and consumers.
// The total amount of publishers is distributed evenly across all nodes in the cluster to stress all nodes equally.
//
// Publishers: Send messages as fast as possible without waiting for confirms
// Consumers: Consume messages as fast as possible and ack them in order to free up memory and avoid blocks on the nodes
// Metrics: Throughput (msgs/sec)
package experiments

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"rmq-benchmark/metrics"
	"rmq-benchmark/rmq"
	"sync"
	"time"

	"github.com/go-faker/faker/v4"
	amqp "github.com/rabbitmq/amqp091-go"
)

type LinearCapacity struct {
	config   Config
	ctrl     *rmq.Controller
	conns    []*amqp.Connection
	queues   []string
	payloads [][]byte
}

func (e *LinearCapacity) Name() string {
	return "linear-capacity"
}

func (e *LinearCapacity) Setup(config Config, ctrl *rmq.Controller) error {
	e.config = config
	e.ctrl = ctrl

	// Pre-generate 1000 payloads with random sizes (1KB to 5KB)
	// => prevents delays in the worker routines during the measurement
	log.Println("Generating test data...")
	e.payloads = make([][]byte, 1000)
	for i := range e.payloads {
		size := 1024 + rand.Intn(4096)
		text := faker.Paragraph()
		b := make([]byte, size)
		copy(b, text)
		if len(text) < size {
			rand.Read(b[len(text):])
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

	e.conns = make([]*amqp.Connection, len(nodes))
	e.queues = make([]string, len(nodes))

	// Connect to each discovered node and create a new quorum queue
	for i, node := range nodes {
		// Connect to each node
		connURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), node)
		conn, err := ctrl.Connect(connURL)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", node, err)
		}
		e.conns[i] = conn

		ch, err := conn.Channel()
		if err != nil {
			return err
		}

		// Set the queue name based on the given base name and node index
		// e.g. queue "test-queue" becomes "test-queue-0", "test-queue-1", ... on each node
		qName := fmt.Sprintf("%s-%d", config.QueueName, i)
		e.queues[i] = qName

		// Configure the queue as a quorum queue with the specified initial group size.
		args := amqp.Table{
			"x-queue-type":                "quorum",
			"x-quorum-initial-group-size": config.QuorumSize,
		}

		// Create the quorum queue (or modify an existing one with the same name)
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
			return err
		}
	}

	return nil
}

func (e *LinearCapacity) Run(ctx context.Context, workers int, rec *metrics.Recorder) (metrics.Summary, error) {
	var wg sync.WaitGroup

	// Start a routine for each connection to listen for amqp.Blocking alarms.
	// These alarms (memory/disk pressure) are logged to detect conditions that
	// invalidate the measurements
	// => https://www.rabbitmq.com/docs/connection-blocked
	for i, conn := range e.conns {
		currentConn := conn
		nodeName := fmt.Sprintf("Node-%d", i)

		blockings := currentConn.NotifyBlocked(make(chan amqp.Blocking))

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
	}

	// --- Start Consumers ---
	// Distribute the total number of consumers equally across all created queues
	if e.config.Consumers > 0 {
		consumersPerQueue := e.config.Consumers / len(e.queues)
		if consumersPerQueue == 0 && e.config.Consumers > 0 {
			consumersPerQueue = 1
		}

		log.Printf("Starting %d consumers per queue (Total: %d)...", consumersPerQueue, e.config.Consumers)

		for i, conn := range e.conns {
			queueName := e.queues[i]

			for c := 0; c < consumersPerQueue; c++ {
				wg.Add(1)
				go func(conn *amqp.Connection, q string) {
					defer wg.Done()

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
				}(conn, queueName)
			}
		}
	}

	// --- Start Publishers ---
	// Distribute the total number of workers equally across all queues
	workersPerNode := workers / len(e.conns)
	if workersPerNode == 0 {
		workersPerNode = 1
	}

	for i, conn := range e.conns {
		queueName := e.queues[i]

		for w := 0; w < workersPerNode; w++ {
			wg.Add(1)
			go func(c *amqp.Connection, q string) {
				defer wg.Done()
				ch, err := c.Channel()
				if err != nil {
					log.Printf("Worker failed to create channel: %v", err)
					return
				}
				defer ch.Close()

				if err := ch.Confirm(false); err != nil {
					log.Printf("Worker failed to enable confirms: %v", err)
					return
				}

				// Pipelined publishing: buffered channel allows 2000 messages in-flight
				// Allows publishers to continue sending without waiting for acks from rmq (acks handled asynchronously)
				// => maximizes throughput
				confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 2000))

				// Background routing to handle the async acks from rmq
				go func() {
					for conf := range confirms {
						if conf.Ack {
							// For capacity tests, we measure throughput not individual latency
							rec.RecordLatency(0)
						} else {
							rec.RecordError()
						}
					}
				}()

				// Non-blocking publish loop
				for {
					select {
					case <-ctx.Done():
						return
					default:
						// Select a random, pre-generated payload that we created during setup
						payload := e.payloads[rand.Intn(len(e.payloads))]

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
			}(conn, queueName)
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
