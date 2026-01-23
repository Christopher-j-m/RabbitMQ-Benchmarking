// Defines the interface that every benchmark experiment must implement to be
// compatible with the benchmarking framework.
package experiments

import (
	"context"
	"rmq-benchmark/metrics"
	"rmq-benchmark/rmq"
)

// Config type that holds configuration parameters for an experiment
type Config struct {
	RabbitURL             string
	ManagementURL         string
	User                  string
	Password              string
	QueueName             string
	QuorumSize            int
	MsgSize               int
	WarmupSeconds         int
	DurationSeconds       int
	Publishers            int
	Consumers             int
	QueueMaxLength        int
	QueueOverflowStrategy string
	QueueCount            int
}

// Interface for defining a experiment
// All experiments must implement these methods in order to be compatible with this rmq benchmark
type Experiment interface {
	// Setup initializes connections, pre-generates data and queues, depending on the experiment requirements.
	Setup(config Config, ctrl *rmq.Controller) error
	// Run executes the corresponding experiment typically including starting monitoring, consumers and publishers.
	Run(ctx context.Context, publishers int, rec *metrics.Recorder) (metrics.Summary, error)
	// Teardown cleans up any resources used during the experiment such as connections and queues.
	Teardown() error
}
