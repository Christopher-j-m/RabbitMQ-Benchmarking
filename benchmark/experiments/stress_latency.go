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
	config          Config
	ctrl            *rmq.Controller
	leaderConn      *amqp.Connection   // Connection to leader for latency measurement
	leaderNodeIndex int                // Index of the leader node in the nodes list
	stressConns     []*amqp.Connection // Connections to all nodes for stress generation
	stressQueues    []string           // Queues for stress traffic (one per node)
	payloads        [][]byte
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

	// Connect directly to the leader node for latency measurement
	leaderURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), leaderNode)
	e.leaderConn, err = ctrl.Connect(leaderURL)
	if err != nil {
		return fmt.Errorf("failed to connect to leader node %s: %w", leaderNode, err)
	}

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

	e.stressConns = make([]*amqp.Connection, len(nodes))
	e.stressQueues = make([]string, len(nodes))

	// Connect to each node and create separate stress queues
	for i, node := range nodes {
		connURL := fmt.Sprintf("amqp://%s:%s@%s:5672/", url.QueryEscape(config.User), url.QueryEscape(config.Password), node)
		stressConn, err := ctrl.Connect(connURL)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", node, err)
		}
		e.stressConns[i] = stressConn

		stressCh, err := stressConn.Channel()
		if err != nil {
			return err
		}

		// Create stress queues with naming convention: <base-queue>-stress-<index>
		qName := fmt.Sprintf("%s-stress-%d", config.QueueName, i)
		e.stressQueues[i] = qName

		_, err = stressCh.QueueDeclare(
			qName, // queue name
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			args,  // same quorum args
		)
		stressCh.Close()
		if err != nil {
			return err
		}
	}

	log.Printf("Created %d stress queues across cluster nodes", len(e.stressQueues))
	return nil
}

func (e *StressLatency) Run(ctx context.Context, publishers int, rec *metrics.Recorder) (metrics.Summary, error) {
	var wg sync.WaitGroup

	// PUBLISHERS
	// Leader Node:
	// - Latency Publishers: Fixed at 1 publisher
	// - Stress Publishers: as specified via 'publishers' param
	// Non-leader Nodes:
	// - Stress Publishers: as specified via 'publishers' param
	const latencyPublishers = 1
	stressPublishersPerNode := publishers

	log.Printf("Publisher Distribution:")
	log.Printf("  - Leader Node: %d Latency + %d Stress Publishers", latencyPublishers, stressPublishersPerNode)
	log.Printf("  - Other Nodes: %d Stress Publishers", stressPublishersPerNode)

	// Listen for blocking alarms on the leader connection
	blockings := e.leaderConn.NotifyBlocked(make(chan amqp.Blocking))
	go func() {
		var blockStart time.Time
		for b := range blockings {
			if b.Active {
				blockStart = time.Now()
				log.Printf("[BLOCKED] Leader - Reason: %q", b.Reason)
			} else {
				duration := time.Since(blockStart)
				log.Printf("[UNBLOCKED] Leader - Duration: %v", duration)
			}
		}
	}()

	// Listen for blocking alarms on all stress connections
	for i, conn := range e.stressConns {
		currentConn := conn
		nodeName := fmt.Sprintf("StressNode-%d", i)
		stressBlockings := currentConn.NotifyBlocked(make(chan amqp.Blocking))
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
		}(nodeName, stressBlockings)
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

		log.Printf("Consumer Distribution:")
		log.Printf("  - Leader Node: %d Latency + %d Stress Consumers", latencyConsumers, stressConsumersPerNode)
		log.Printf("  - Other Nodes: %d Stress Consumers", stressConsumersPerNode)

		// Start Latency Consumer (Leader only)
		wg.Add(1)
		go e.runConsumer(ctx, &wg, e.leaderConn, e.config.QueueName, rec)

		// Start Stress Consumers on all nodes
		for i, conn := range e.stressConns {
			queueName := e.stressQueues[i]

			for c := 0; c < stressConsumersPerNode; c++ {
				wg.Add(1)
				go e.runConsumer(ctx, &wg, conn, queueName, nil)
			}
		}
	} else {
		log.Println("WARNING: Consumers set to < 0, so no consumers will be started...")
	}

	// Start Stress Publishers
	stressCounter := 0
	for i, conn := range e.stressConns {
		queueName := e.stressQueues[i]

		for w := 0; w < stressPublishersPerNode; w++ {
			publisherID := stressCounter
			stressCounter++
			wg.Add(1)
			go e.runStressPublisher(ctx, &wg, conn, queueName, publisherID)
		}
	}

	// Start Latency Publishers
	log.Printf("Starting %d latency publisher...", latencyPublishers)
	for i := 0; i < latencyPublishers; i++ {
		wg.Add(1)
		go e.runLatencyPublisher(ctx, &wg, i, rec)
	}

	// Wait for the go context to be canceled and all coroutines to exit gracefully
	wg.Wait()
	return rec.GetSummary(), nil
}

// Drain messages from a queue
// If rec is not nil (latency consumer), it extracts the timestamp from headers and records latency
func (e *StressLatency) runConsumer(ctx context.Context, wg *sync.WaitGroup, conn *amqp.Connection, queueName string, rec *metrics.Recorder) {
	defer wg.Done()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Consumer failed to create channel: %v", err)
		return
	}
	defer ch.Close()

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

	for {
		select {
		case <-ctx.Done():
			// Flush remaining latencies
			if rec != nil && len(latencyBatch) > 0 {
				rec.RecordLatencyBatch(latencyBatch)
			}
			return
		case d, ok := <-msgs:
			if !ok {
				if rec != nil && len(latencyBatch) > 0 {
					rec.RecordLatencyBatch(latencyBatch)
				}
				return
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
	}
}

// Fire messages as fast as possible without waiting for confirms
func (e *StressLatency) runStressPublisher(ctx context.Context, wg *sync.WaitGroup, conn *amqp.Connection, queueName string, publisherID int) {
	defer wg.Done()

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

// Send messages synchronously, waiting for ACK & with timestamp header
func (e *StressLatency) runLatencyPublisher(ctx context.Context, wg *sync.WaitGroup, publisherID int, rec *metrics.Recorder) {
	defer wg.Done()

	rng := rand.New(rand.NewPCG(uint64(publisherID), uint64(time.Now().UnixNano())))

	ch, err := e.leaderConn.Channel()
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
	var lastErr error

	if e.leaderConn != nil {
		if err := e.leaderConn.Close(); err != nil {
			lastErr = err
		}
	}

	for _, conn := range e.stressConns {
		if conn != nil {
			if err := conn.Close(); err != nil {
				lastErr = err
			}
		}
	}

	return lastErr
}
