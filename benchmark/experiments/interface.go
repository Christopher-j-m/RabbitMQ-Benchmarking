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
	RabbitURL       string
	ManagementURL   string
	User            string
	Password        string
	QueueName       string
	QuorumSize      int
	MsgSize         int
	WarmupSeconds   int
	DurationSeconds int
	Consumers       int
}

// Summarized results after an experiment run
type ExperimentResult struct {
	MeanThroughput   float64
	StdDevThroughput float64
	P99Latency       int64
	ErrorCount       int64
}

// Interface for defining a experiment
// All experiments must implement these methods in order to be compatible with this rmq benchmark
type Experiment interface {
	// Name should return the unique identifier name of the experiment.
	Name() string
	// Setup initializes connections, pre-generates data and queues, depending on the experiment requirements.
	Setup(config Config, ctrl *rmq.Controller) error
	// Run executes the corresponding experiment typically including starting monitoring, consumers and publishers.
	Run(ctx context.Context, workers int, rec *metrics.Recorder) (metrics.Summary, error)
	// Teardown cleans up any resources used during the experiment such as connections and queues.
	Teardown() error
}
