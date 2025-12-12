// Raft Tax Experiment
//
// Measures the latency cost imposed by the Raft consensus mechanism
// in rmq quorum queues. This experiment forces publishers to wait for a
// confirmed write (ACK) from the cluster for every message, measuring the
// round-trip time required for successful replication across replicas in the cluster.
//
// Publishers: Send messages synchronously, waiting for a ack before sending the next one.
// Consumers: Consume messages immediately to prevent queue backlog from influencing latency measurements.
// Metrics: Latency (P99, P95, Mean) and Throughput (msgs/sec)
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

type RaftTax struct {
	config   Config
	ctrl     *rmq.Controller
	conn     *amqp.Connection
	payloads [][]byte
}

func (e *RaftTax) Name() string {
	return "raft-tax"
}

func (e *RaftTax) Setup(config Config, ctrl *rmq.Controller) error {
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

	// Connect to cluster to declare queue
	conn, err := ctrl.Connect(config.RabbitURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Configure the queue as a quorum queue with the specified initial group size.
	args := amqp.Table{
		"x-queue-type":                "quorum",
		"x-quorum-initial-group-size": config.QuorumSize,
	}

	// Create the quorum queue (or modify an existing one with the same name)
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

	// Connect directly to the leader node
	leaderURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), leaderNode)
	e.conn, err = ctrl.Connect(leaderURL)
	if err != nil {
		return fmt.Errorf("failed to connect to leader node %s: %w", leaderNode, err)
	}

	return nil
}

func (e *RaftTax) Run(ctx context.Context, workers int, rec *metrics.Recorder) (metrics.Summary, error) {
	var wg sync.WaitGroup

	// Listen for amqp.Blocking alarms on the connection with the leader node.
	// These alarms (memory/disk pressure) are logged to detect conditions that
	// invalidate the measurements
	// => https://www.rabbitmq.com/docs/connection-blocked
	blockings := e.conn.NotifyBlocked(make(chan amqp.Blocking))

	go func() {
		var blockStart time.Time
		for b := range blockings {
			if b.Active {
				blockStart = time.Now()
				log.Printf("[BLOCKED] Reason: %q", b.Reason)
			} else {
				duration := time.Since(blockStart)
				log.Printf("[UNBLOCKED] Duration: %v", duration)
			}
		}
	}()

	// --- Start Consumers ---
	// Consumers are started to drain the queue immediately.
	// This prevents the queue from building up back pressure,
	// isolating the latency measurement to Raft consensus (and not queue depth).
	if e.config.Consumers > 0 {
		log.Printf("Starting %d consumers...", e.config.Consumers)
		for i := 0; i < e.config.Consumers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				ch, err := e.conn.Channel()
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
				msgs, err := ch.Consume(
					e.config.QueueName, // queue
					"",                 // consumer - unique consumer identifier
					false,              // auto-ack - consumer must ack msgs
					false,              // exclusive - not the only consumer for this queue
					false,              // no-local - allow consuming msgs from the same connection
					false,              // no-wait - wait for rmq confirmation that consumer is registered
					nil,                // optional args
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
			}()
		}
	}
	
	// --- Start Publishers ---
    // Workers publish and wait for a ack for every message
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, err := e.conn.Channel()
			if err != nil {
				log.Printf("Worker failed to create channel: %v", err)
				return
			}
			defer ch.Close()

			// Enable Publisher Confirms on this channel
			// RMQ sends an ack for every published message once it is
			// replicated to all the other nodes (quorum)
			// => Mandatory for measuring the Raft replication latency,
			// ... which is the main objective of this experiment
			if err := ch.Confirm(false); err != nil {
				log.Printf("Worker failed to enable confirms: %v", err)
				return
			}

			// Create a specific channel for this specific publisher confirmations
			// Buffer size 1: Publisher can't send another message until the previous one
			// is confirmed by rmq
			// => Message throughput is effectively throttled by the Raft consensus latency
			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					payload := e.payloads[rand.Intn(len(e.payloads))]

					// Start Timer
					start := time.Now()

					// Publish the message
					err := ch.PublishWithContext(ctx,
						"",                 // exchange - default: route to queue with name equal to routing key
                        e.config.QueueName, // routing key (queue name)
                        false,              // mandatory - unroutable messages are dropped
                        false,              // immediate - no immediate delivery
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        payload,
						},
					)

					if err != nil {
						if err == amqp.ErrClosed {
							return
						}
						// Backoff on error
						rec.RecordError()
						time.Sleep(50 * time.Millisecond) 
						continue
					}

					// Block until the Raft consensus completes and the server sends an ACK.
                    // This time is recorded as the publish latency.
					select {
					case conf := <-confirms:
						if conf.Ack {
							// Record the full round-trip time
							rec.RecordLatency(time.Since(start))
						} else {
							// Nack or error
							rec.RecordError()
						}
					
					// Handle cases where rmq is overloaded and doesn't respond in time
					// This allows us to capture latency spikes without erroring out
					case <-time.After(30 * time.Second):
						log.Printf("Worker timed out waiting for confirm")
						rec.RecordError()

						// Our last resort: backoff for 1-3 seconds before trying again
						time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
					}
				}
			}
		}()
	}

	// Wait for the go context to be canceled and all coroutines to exit gracefully
	wg.Wait()
	return rec.GetSummary(), nil
}

func (e *RaftTax) Teardown() error {
	if e.conn != nil {
		return e.conn.Close()
	}
	return nil
}
