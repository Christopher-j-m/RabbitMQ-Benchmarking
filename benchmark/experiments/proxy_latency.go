// Proxy Latency Experiment
//
// Measures the additional latency cost imposed by publishing through a non-leader
// node in rmq quorum queues, while using the same synchronous publishing pattern as
// ideal_latency.
//
// Publishers: Send messages synchronously to a non-leader node, waiting for ACK before sending the next one.
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

	amqp "github.com/rabbitmq/amqp091-go"
)

type ProxyLatency struct {
	config     Config
	controller *rmq.Controller
	connection *amqp.Connection
	payloads   [][]byte
}

func (experiment *ProxyLatency) Setup(config Config, controller *rmq.Controller) error {
	experiment.config = config
	experiment.controller = controller

	// Pre-generate 1000 random payloads with with the specified fixed size
	// => prevents delays in the publisher routines during the measurement
	log.Println("Generating test data...")
	experiment.payloads = GeneratePayloads(1000, config.MsgSize)
	log.Println("Data generation complete. Connecting to RabbitMQ...")

	// Connect to cluster to declare queue
	tempConnection, err := controller.Connect(config.RabbitURL)
	if err != nil {
		return err
	}
	amqpChannel, err := tempConnection.Channel()
	if err != nil {
		_ = tempConnection.Close()
		return err
	}

	// Configure the queue as a quorum queue with the specified initial group size.
	args := amqp.Table{
		"x-queue-type":                "quorum",
		"x-quorum-initial-group-size": config.QuorumSize,
	}

	_, err = amqpChannel.QueueDeclare(
		config.QueueName, // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		args,             // arguments
	)
	_ = amqpChannel.Close()
	_ = tempConnection.Close()
	if err != nil {
		return err
	}

	// Find the leader node for the created quorum queue
	leaderNode, err := controller.GetQueueLeaderNode("/", config.QueueName)
	if err != nil {
		return err
	}

	// Discover all cluster nodes and find a non-leader node
	nodes, err := controller.GetNodes()
	if err != nil {
		return err
	}

	// Select the first non-leader node
	var nonLeaderNode string
	for _, n := range nodes {
		if n != leaderNode {
			nonLeaderNode = n
			break
		}
	}

	if nonLeaderNode == "" {
		return fmt.Errorf("could not find a non-leader node (cluster size < 2?)")
	}

	log.Printf("Queue leader is on node: %s", leaderNode)
	log.Printf("Connecting to non-leader node: %s", nonLeaderNode)

	// Connect to the chosen non-leader node
	nonLeaderURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), nonLeaderNode)
	experiment.connection, err = controller.Connect(nonLeaderURL)
	if err != nil {
		return fmt.Errorf("failed to connect to non-leader node %s: %w", nonLeaderNode, err)
	}

	return nil
}

func (experiment *ProxyLatency) Run(ctx context.Context, publishers int, metricsRecorder *metrics.Recorder) (metrics.Summary, error) {
	var waitGroup sync.WaitGroup

	// Listen for amqp.Blocking alarms on the connection with the non-leader node.
	// These alarms (memory/disk pressure) are logged to detect conditions that
	// invalidate the measurements
	// => https://www.rabbitmq.com/docs/connection-blocked
	blockings := experiment.connection.NotifyBlocked(make(chan amqp.Blocking))

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

	// CONSUMERS
	// Consumers are started to drain the queue immediately.
	// This prevents the queue from building up back pressure,
	// isolating the latency measurement to Raft consensus + proxy overhead (and not queue depth).
	if experiment.config.Consumers > 0 {
		log.Printf("Starting %d consumers...", experiment.config.Consumers)
		for i := 0; i < experiment.config.Consumers; i++ {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()

				amqpChannel, err := experiment.connection.Channel()
				if err != nil {
					log.Printf("Consumer failed to create channel: %v", err)
					return
				}
				defer func() { _ = amqpChannel.Close() }()

				// Set QoS for the consumer channel to prefetch 50 messages
				// Consumers will receive up to 50 unacknowledged messages at a time
				// => Prevents flooding the consumer with too many messages
				if err := amqpChannel.Qos(50, 0, false); err != nil {
					log.Printf("Consumer failed to set QoS: %v", err)
					return
				}

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

				// Acknowledge messages as they arrive
				for {
					select {
					case <-ctx.Done():
						return
					case delivery, ok := <-messages:
						if !ok {
							return
						}
						_ = delivery.Ack(false)
					}
				}
			}()
		}
	}

	// PUBLISHERS
	// Publishers publish synchronously and wait for an ACK for every message.
	log.Printf("Starting %d publishers...", publishers)
	for i := 0; i < publishers; i++ {
		waitGroup.Add(1)
		go func(publisherID int) {
			defer waitGroup.Done()

			// Local RNG to avoid global mutex contention
			// => https://go.dev/blog/randv2
			randomGenerator := rand.New(rand.NewPCG(uint64(publisherID), uint64(time.Now().UnixNano())))

			amqpChannel, err := experiment.connection.Channel()
			if err != nil {
				log.Printf("Publisher failed to create channel: %v", err)
				return
			}
			defer func() { _ = amqpChannel.Close() }()

			// Enable Publisher Confirms on this channel
			// RMQ sends an ack for every published message once it is
			// replicated to all the other nodes (quorum)
			// Since we're connected to a non-leader, the message must first
			// be forwarded to the leader, adding extra latency that we're
			// measuring in this experiment
			if err := amqpChannel.Confirm(false); err != nil {
				log.Printf("Publisher failed to enable confirms: %v", err)
				return
			}

			// Create a specific channel for this specific publisher confirmations
			// Buffer size 1: Publisher can't send another message until the previous one
			// is confirmed by rmq
			// => Message throughput is effectively throttled by the Raft consensus + proxy latency
			confirmations := amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1))

			// Local batch for latency recording
			const batchSize = 100
			latencyBatch := make([]int64, 0, batchSize)

			for {
				select {
				case <-ctx.Done():
					// Flush remaining latencies
					if len(latencyBatch) > 0 {
						metricsRecorder.RecordLatencyBatch(latencyBatch)
					}
					return
				default:
					payload := experiment.payloads[randomGenerator.IntN(len(experiment.payloads))]

					// Start Timer
					start := time.Now()

					// Publish the message
					err := amqpChannel.PublishWithContext(ctx,
						"",                          // exchange - default: route to queue with name equal to routing key
						experiment.config.QueueName, // routing key (queue name)
						false,                       // mandatory - unroutable messages are dropped
						false,                       // immediate - no immediate delivery
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        payload,
						},
					)

					if err != nil {
						if err == amqp.ErrClosed {
							if len(latencyBatch) > 0 {
								metricsRecorder.RecordLatencyBatch(latencyBatch)
							}
							return
						}
						// Backoff on error
						metricsRecorder.RecordError()
						time.Sleep(50 * time.Millisecond)
						continue
					}

					// Block until the Raft consensus completes and the server sends an ACK.
					// This time includes: follower -> leader forwarding + Raft consensus + ACK return
					select {
					case <-ctx.Done():
						return
					case confirmation := <-confirmations:
						if confirmation.Ack {
							// Record the full round-trip time (includes proxy overhead)
							latencyBatch = append(latencyBatch, time.Since(start).Microseconds())
							if len(latencyBatch) >= batchSize {
								metricsRecorder.RecordLatencyBatch(latencyBatch)
								latencyBatch = latencyBatch[:0]
							}
						} else {
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
		}(i)
	}

	// Wait for the go context to be canceled and all coroutines to exit gracefully
	waitGroup.Wait()
	return metricsRecorder.GetSummary(), nil
}

func (experiment *ProxyLatency) Teardown() error {
	if experiment.connection != nil {
		return experiment.connection.Close()
	}
	return nil
}
