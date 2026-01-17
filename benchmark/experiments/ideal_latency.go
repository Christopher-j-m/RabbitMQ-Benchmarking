// Raft Latency Experiment
//
// Measures the latency cost imposed by the Raft consensus in rmq quorum queues.
// This experiment forces publishers to wait for a confirmed write (ACK)
// from the cluster for every message, measuring the round-trip time required for
// successful replication across replicas in the cluster.
// => Inject timestamp into message headers for end-to-end latency calculation.
//
// Publishers: Send messages synchronously, waiting for a ack before sending the next one.
// Consumers: Consume messages immediately to prevent queue backlog from influencing latency measurements.
// Metrics: Latency (P99, P95, Mean)
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

type IdealLatency struct {
	config    Config
	ctrl      *rmq.Controller
	leaderURL string
	conns     []*amqp.Connection
	payloads  [][]byte
}

func (e *IdealLatency) Setup(config Config, ctrl *rmq.Controller) error {
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

	// Connect to cluster
	conn, err := ctrl.Connect(config.RabbitURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Delete existing queues with the same name as the ones
	// that will be created during this experiment
	chDelete, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel for deletion: %w", err)
	}

	log.Printf("Deleting existing queue %s...", config.QueueName)
	_, _ = chDelete.QueueDelete(config.QueueName, false, false, false)
	chDelete.Close()

	// Create a channel for queue declaration
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

	// Create the quorum queue
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

	// Store leader URL for publisher/consumer connections
	e.leaderURL = fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), leaderNode)
	e.conns = make([]*amqp.Connection, 0)

	return nil
}

func (e *IdealLatency) Run(ctx context.Context, publishers int, rec *metrics.Recorder) (metrics.Summary, error) {
	var wg sync.WaitGroup

	// Monitor cnnection for blocked notifications
	monitorConnection := func(conn *amqp.Connection) {
		blockings := conn.NotifyBlocked(make(chan amqp.Blocking))
		go func(c <-chan amqp.Blocking) {
			var blockStart time.Time
			for b := range c {
				if b.Active {
					blockStart = time.Now()
					log.Printf("[BLOCKED] Reason: %q", b.Reason)
				} else {
					duration := time.Since(blockStart)
					log.Printf("[UNBLOCKED] Duration: %v", duration)
				}
			}
		}(blockings)
	}

	// Create Consumer Connection
	consumerConn, err := e.ctrl.Connect(e.leaderURL)
	if err != nil {
		return metrics.Summary{}, fmt.Errorf("consumer failed to create connection: %w", err)
	}
	e.conns = append(e.conns, consumerConn)
	monitorConnection(consumerConn)

	// Create Publisher Connection
	publisherConn, err := e.ctrl.Connect(e.leaderURL)
	if err != nil {
		return metrics.Summary{}, fmt.Errorf("publisher failed to create connection: %w", err)
	}
	e.conns = append(e.conns, publisherConn)
	monitorConnection(publisherConn)

	// CONSUMER
	// Consumer is started to drain the queue immediately and record end-to-end latency.
	// It extracts the timestamp from the message header and calculates the latency.
	log.Printf("Starting 1 consumer (fixed for Ideal Latency)...")

	wg.Add(1)
	go func(conn *amqp.Connection, rec *metrics.Recorder) {
		defer wg.Done()

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("Consumer failed to create channel: %v", err)
			return
		}
		defer ch.Close()

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

		// Local batch for latency recording to avoid lock contention
		const batchSize = 100
		latencyBatch := make([]int64, 0, batchSize)

		// Consume messages and record end-to-end latency
		for {
			select {
			case <-ctx.Done():
				// Flush remaining latencies
				if len(latencyBatch) > 0 {
					rec.RecordLatencyBatch(latencyBatch)
				}
				return
			case d, ok := <-msgs:
				if !ok {
					if len(latencyBatch) > 0 {
						rec.RecordLatencyBatch(latencyBatch)
					}
					return
				}

				// Extract timestamp from header and calculate end-to-end latency
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

				d.Ack(false)
			}
		}
	}(consumerConn, rec)

	// PUBLISHER
	// Publishes and waits for a ack for every message
	log.Printf("Starting 1 publisher (fixed for Ideal Latency)...")

	wg.Add(1)
	go func(conn *amqp.Connection, publisherID int) {
		defer wg.Done()

		// Local RNG to avoid global mutex contention
		// => https://go.dev/blog/randv2
		rng := rand.New(rand.NewPCG(uint64(publisherID), uint64(time.Now().UnixNano())))

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("Publisher failed to create channel: %v", err)
			return
		}
		defer ch.Close()

		// Enable Publisher Confirms on this channel
		// RMQ sends an ack for every published message once it is
		// replicated to all the other nodes (quorum)
		// => Mandatory for measuring the Raft replication latency,
		// ... which is the main objective of this experiment
		if err := ch.Confirm(false); err != nil {
			log.Printf("Publisher failed to enable confirms: %v", err)
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
				payload := e.payloads[rng.IntN(len(e.payloads))]

				// Publish the message with timestamp header for end-to-end latency calculation
				err := ch.PublishWithContext(ctx,
					"",                 // exchange - default: route to queue with name equal to routing key
					e.config.QueueName, // routing key (queue name)
					false,              // mandatory - unroutable messages are dropped
					false,              // immediate - no immediate delivery
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
					// Backoff on error
					rec.RecordError()
					time.Sleep(50 * time.Millisecond)
					continue
				}

				// Block until the Raft consensus completes and the server sends an ACK.
				// This ensures the cluster is stressed by waiting for replication.
				// Latency is recorded by consumers, not here.
				select {
				case <-ctx.Done():
					return
				case conf := <-confirms:
					if !conf.Ack {
						// Nack or error
						rec.RecordError()
					}

				// Handle cases where rmq is overloaded and doesn't respond in time
				// This allows us to capture latency spikes without erroring out
				case <-time.After(30 * time.Second):
					log.Printf("Publisher timed out waiting for confirm")
					rec.RecordError()

					// Our last resort: backoff for 1-3 seconds before trying again
					time.Sleep(time.Duration(1000+rng.IntN(2000)) * time.Millisecond)
				}
			}
		}
	}(publisherConn, 0)

	// Wait for the go context to be canceled and all coroutines to exit gracefully
	wg.Wait()
	return rec.GetSummary(), nil
}

func (e *IdealLatency) Teardown() error {
	for _, conn := range e.conns {
		if conn != nil {
			conn.Close()
		}
	}
	return nil
}
