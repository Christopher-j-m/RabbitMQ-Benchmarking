// Ideal Latency Experiment
//
// Measures the latency cost imposed by the Raft consensus in rmq quorum queues under ideal conditions
// with a fixed a fixed 1-publisher/1-consumer setup connected directly to the queue leader node.
// Inject timestamp into message headers for end-to-end latency calculation.
//
// Publisher: Send messages synchronously, waiting for a ack before sending the next one.
// Consumer: Extracts 'x-sent-at' headers for latency calculation.
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

	amqp "github.com/rabbitmq/amqp091-go"
)

type IdealLatency struct {
	config      Config
	controller  *rmq.Controller
	leaderURL   string
	connections []*amqp.Connection
	payloads    [][]byte
}

func (experiment *IdealLatency) Setup(config Config, controller *rmq.Controller) error {
	experiment.config = config
	experiment.controller = controller

	// Pre-generate 1000 random payloads with with the specified fixed size
	// => prevents delays in the publisher routines during the measurement
	log.Println("Generating test data...")
	experiment.payloads = GeneratePayloads(1000, config.MsgSize)
	log.Println("Data generation complete. Connecting to RabbitMQ...")

	// Connect to cluster
	connection, err := controller.Connect(config.RabbitURL)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()

	// Delete existing queues with the same name as the ones
	// that will be created during this experiment
	channelDelete, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel for deletion: %w", err)
	}

	log.Printf("Deleting existing queue %s...", config.QueueName)
	_, _ = channelDelete.QueueDelete(config.QueueName, false, false, false)
	_ = channelDelete.Close()

	// Create a channel for queue declaration
	amqpChannel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = amqpChannel.Close() }()

	// Configure the queue as a quorum queue with the specified initial group size.
	args := amqp.Table{
		"x-queue-type":                "quorum",
		"x-quorum-initial-group-size": config.QuorumSize,
	}

	// Create the quorum queue
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

	// Store leader URL for publisher/consumer connections
	experiment.leaderURL = fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), leaderNode)
	experiment.connections = make([]*amqp.Connection, 0)

	return nil
}

func (experiment *IdealLatency) Run(ctx context.Context, publishers int, metricsRecorder *metrics.Recorder) (metrics.Summary, error) {
	var waitGroup sync.WaitGroup

	// Monitor cnnection for blocked notifications
	monitorConnection := func(connection *amqp.Connection) {
		blockings := connection.NotifyBlocked(make(chan amqp.Blocking))
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
	consumerConn, err := experiment.controller.Connect(experiment.leaderURL)
	if err != nil {
		return metrics.Summary{}, fmt.Errorf("consumer failed to create connection: %w", err)
	}
	experiment.connections = append(experiment.connections, consumerConn)
	monitorConnection(consumerConn)

	// Create Publisher Connection
	publisherConn, err := experiment.controller.Connect(experiment.leaderURL)
	if err != nil {
		return metrics.Summary{}, fmt.Errorf("publisher failed to create connection: %w", err)
	}
	experiment.connections = append(experiment.connections, publisherConn)
	monitorConnection(publisherConn)

	// CONSUMER
	// Consumer is started to drain the queue immediately and record end-to-end latency.
	// It extracts the timestamp from the message header and calculates the latency.
	log.Printf("Starting 1 consumer (fixed for Ideal Latency)...")

	waitGroup.Add(1)
	go func(connection *amqp.Connection, metricsRecorder *metrics.Recorder) {
		defer waitGroup.Done()

		amqpChannel, err := connection.Channel()
		if err != nil {
			log.Printf("Consumer failed to create channel: %v", err)
			return
		}
		defer func() { _ = amqpChannel.Close() }()

		// Start consuming messages
		messages, err := amqpChannel.Consume(
			experiment.config.QueueName, // queue name
			"",                          // consumer - unique consumer identifier
			false,                       // auto-ack - consumer must ack msgs
			false,                       // exclusive - not the only consumer for this queue
			false,                       // no-local - allow consuming msgs from the same connection
			false,                       // no-wait - wait for rmq confirmation that consumer is registered
			nil,                         // optional args
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
					metricsRecorder.RecordLatencyBatch(latencyBatch)
				}
				return
			case delivery, ok := <-messages:
				if !ok {
					if len(latencyBatch) > 0 {
						metricsRecorder.RecordLatencyBatch(latencyBatch)
					}
					return
				}

				// Extract timestamp from header and calculate end-to-end latency
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

				_ = delivery.Ack(false)
			}
		}
	}(consumerConn, metricsRecorder)

	// PUBLISHER
	// Publishes and waits for a ack for every message
	log.Printf("Starting 1 publisher (fixed for Ideal Latency)...")

	waitGroup.Add(1)
	go func(connection *amqp.Connection, publisherID int) {
		defer waitGroup.Done()

		// Local RNG to avoid global mutex contention
		// => https://go.dev/blog/randv2
		randomGenerator := rand.New(rand.NewPCG(uint64(publisherID), uint64(time.Now().UnixNano())))

		amqpChannel, err := connection.Channel()
		if err != nil {
			log.Printf("Publisher failed to create channel: %v", err)
			return
		}
		defer func() { _ = amqpChannel.Close() }()

		// Enable Publisher Confirms on this channel
		// RMQ sends an ack for every published message once it is
		// replicated to all the other nodes (quorum)
		// => Mandatory for measuring the Raft replication latency,
		// ... which is the main objective of this experiment
		if err := amqpChannel.Confirm(false); err != nil {
			log.Printf("Publisher failed to enable confirms: %v", err)
			return
		}

		// Create a specific channel for this specific publisher confirmations
		// Buffer size 1: Publisher can't send another message until the previous one
		// is confirmed by rmq
		// => Message throughput is effectively throttled by the Raft consensus latency
		confirmations := amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1))

		for {
			select {
			case <-ctx.Done():
				return
			default:
				payload := experiment.payloads[randomGenerator.IntN(len(experiment.payloads))]

				// Publish the message with timestamp header for end-to-end latency calculation
				err := amqpChannel.PublishWithContext(ctx,
					"",                          // exchange - default: route to queue with name equal to routing key
					experiment.config.QueueName, // routing key (queue name)
					false,                       // mandatory - unroutable messages are dropped
					false,                       // immediate - no immediate delivery
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
					metricsRecorder.RecordError()
					time.Sleep(50 * time.Millisecond)
					continue
				}

				// Block until the Raft consensus completes and the server sends an ACK.
				// This ensures the cluster is stressed by waiting for replication.
				// Latency is recorded by consumers, not here.
				select {
				case <-ctx.Done():
					return
				case confirmation := <-confirmations:
					if !confirmation.Ack {
						// Nack or error
						metricsRecorder.RecordError()
					}

				// Handle cases where rmq is overloaded and doesn't respond in time
				// This allows us to capture latency spikes without erroring out
				case <-time.After(30 * time.Second):
					log.Printf("Publisher timed out waiting for confirm")
					metricsRecorder.RecordError()

					// Our last resort: backoff for 1-3 seconds before trying again
					time.Sleep(time.Duration(1000+randomGenerator.IntN(2000)) * time.Millisecond)
				}
			}
		}
	}(publisherConn, 0)

	// Wait for the go context to be canceled and all coroutines to exit gracefully
	waitGroup.Wait()
	return metricsRecorder.GetSummary(), nil
}

func (experiment *IdealLatency) Teardown() error {
	for _, connection := range experiment.connections {
		if connection != nil {
			_ = connection.Close()
		}
	}
	return nil
}
