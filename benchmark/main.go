// Parses command-line params, sets up logging, initializes the experiments and finally executes the benchmark.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"rmq-benchmark/experiments"
	"rmq-benchmark/metrics"
	"rmq-benchmark/monitor"
	"rmq-benchmark/rmq"
	"time"

	"github.com/spf13/cobra"
)

// Command-line parameters
var (
	mgmtURL               string
	rmqUser               string
	rmqPassword           string
	queueName             string
	quorumSize            int
	msgSize               int
	warmup                int
	duration              int
	publishers            int
	consumers             int
	experimentName        string
	queueMaxLength        int
	queueOverflowStrategy string
	queueCount            int
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "rmq-benchmark",
		Short: "RabbitMQ Benchmarking Tool",
		Run:   runBenchmark,
	}

	// CLI params and their default values
	rootCmd.Flags().StringVar(&mgmtURL, "mgmt-url", "", "RabbitMQ Management URL (Required)")
	rootCmd.Flags().StringVar(&rmqUser, "rmq-user", "", "RabbitMQ User (Required)")
	rootCmd.Flags().StringVar(&rmqPassword, "rmq-password", "", "RabbitMQ Password (Required)")
	rootCmd.Flags().StringVar(&queueName, "queue-name", "benchmark-queue", "Queue Name")
	rootCmd.Flags().IntVar(&quorumSize, "quorum-size", 3, "Quorum Queue Group Size")
	rootCmd.Flags().IntVar(&msgSize, "msg-size", 1024, "Message Size in Bytes")
	rootCmd.Flags().IntVar(&warmup, "warmup", 60, "Warmup duration in seconds")
	rootCmd.Flags().IntVar(&duration, "duration", 600, "Test duration in seconds")
	rootCmd.Flags().IntVar(&publishers, "publishers", 10, "Number of concurrent publishers per node")
	rootCmd.Flags().IntVar(&consumers, "consumers", 0, "Number of concurrent consumers per node")
	rootCmd.Flags().IntVar(&queueMaxLength, "queue-length", 0, "Maximum queue length (0 = unlimited)")
	rootCmd.Flags().StringVar(&queueOverflowStrategy, "queue-overflow", "drop-head", "Queue overflow behavior when queue-length > 0: 'drop-head', 'reject-publish', or 'reject-publish-dlx'")
	rootCmd.Flags().IntVar(&queueCount, "queue-count", 1, "Number of queues to create per node")

	experimentsList := experiments.ListExperiments()
	helpText := fmt.Sprintf("Experiment to run: %v (Required)", experimentsList)
	rootCmd.Flags().StringVar(&experimentName, "experiment", "", helpText)

	// Mark flags as required and throw error if any are missing
	// We require users to explicitly specify these parameters
	rootCmd.MarkFlagRequired("mgmt-url")
	rootCmd.MarkFlagRequired("rmq-user")
	rootCmd.MarkFlagRequired("rmq-password")
	rootCmd.MarkFlagRequired("experiment")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Log terminating error to both stdout and log file before exiting
func fatalf(format string, v ...interface{}) {
	logAndPrint(format, v...)
	os.Exit(1)
}

// Log a message to both the log file and stdout
func logAndPrint(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Print(msg)
	fmt.Println(msg)
}

// Build the AMQP connection string from the existing parameters (management URL and credentials)
func deriveAMQPURL(mgmtURL, user, password string) (string, error) {
	parsed, err := url.Parse(mgmtURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse management URL: %w", err)
	}
	host := parsed.Hostname()
	return fmt.Sprintf("amqp://%s:%s@%s:5672/", user, password, host), nil
}

// Main benchmark execution
func runBenchmark(cmd *cobra.Command, args []string) {
	// Create logs directory at ./logs and create log file
	logsDir := "logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		fmt.Printf("Failed to create logs directory: %v\n", err)
		os.Exit(1)
	}

	// Log file named in the following format: benchmark_<experiment>_<timestamp>.log
	timestamp := time.Now().Format("20060102-150405")
	logFile := filepath.Join(logsDir, fmt.Sprintf("benchmark_%s_%s.log", experimentName, timestamp))
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Log from now on per default only to log file to avoid interfering with progress bar
	log.SetOutput(f)

	amqpURL, err := deriveAMQPURL(mgmtURL, rmqUser, rmqPassword)
	if err != nil {
		fatalf("Failed to construct AMQP URL from the given parameters: %v", err)
	}

	// Ensure that the provided queue-overflow value is a valid strategy
	if queueMaxLength > 0 && queueOverflowStrategy != "" {
		validOverflows := map[string]bool{
			"drop-head":          true,
			"reject-publish":     true,
			"reject-publish-dlx": true,
		}
		if !validOverflows[queueOverflowStrategy] {
			fatalf("Invalid queue-overflow: %s. Valid values are: drop-head, reject-publish, reject-publish-dlx", queueOverflowStrategy)
		}
	}

	// Ensure that the queue count is at least 1
	if queueCount < 1 {
		fatalf("Invalid queue-count: %d. Must be at least 1.", queueCount)
	}

	// Log CLI params to both stdout and log file
	logAndPrint("---------------------------------------------------")
	logAndPrint("Benchmark Parameters:")
	logAndPrint("Experiment: %s", experimentName)
	logAndPrint("Mgmt URL: %s", mgmtURL)
	logAndPrint("Rmq User: %s", rmqUser)
	logAndPrint("Queue Name: %s", queueName)
	logAndPrint("Queue Count: %d", queueCount)
	logAndPrint("Quorum Size: %d", quorumSize)
	logAndPrint("Msg Size (bytes): %d", msgSize)
	logAndPrint("Warmup (seconds): %d", warmup)
	logAndPrint("Duration (seconds): %d", duration)
	logAndPrint("Publishers: %d", publishers)
	logAndPrint("Consumers: %d", consumers)
	if queueMaxLength > 0 {
		logAndPrint("Queue Length: %d", queueMaxLength)
		logAndPrint("Queue Overflow: %s", queueOverflowStrategy)
	}
	logAndPrint("---------------------------------------------------")

	// Controller component contains common functionality for RabbitMQ node & Management API interactions
	ctrl := rmq.NewController(mgmtURL, rmqUser, rmqPassword)

	// Create specified experiment & pass parameters
	exp, err := experiments.GetExperiment(experimentName)
	if err != nil {
		fatalf("Failed to select the specified experiment: %v", err)
	}

	config := experiments.Config{
		RabbitURL:             amqpURL,
		ManagementURL:         mgmtURL,
		User:                  rmqUser,
		Password:              rmqPassword,
		QueueName:             queueName,
		QuorumSize:            quorumSize,
		MsgSize:               msgSize,
		WarmupSeconds:         warmup,
		DurationSeconds:       duration,
		Publishers:            publishers,
		Consumers:             consumers,
		QueueMaxLength:        queueMaxLength,
		QueueOverflowStrategy: queueOverflowStrategy,
		QueueCount:            queueCount,
	}
	if err := exp.Setup(config, ctrl); err != nil {
		fatalf("Failed to configure the experiment: %v", err)
	}
	defer exp.Teardown()

	// Create the Metrics Recorder & start recording
	// TODO: Make custom Recorders defineable per experiment (to keep being extensible)
	rec, err := metrics.NewRecorder(experimentName, quorumSize, warmup)
	if err != nil {
		fatalf("Failed to create recorder: %v", err)
	}

	// Write cli parameters into config file alongside results for traceability
	benchmarkConfig := metrics.BenchmarkConfig{
		Experiment:            experimentName,
		StartTime:             time.Now().Format(time.RFC3339),
		ManagementURL:         mgmtURL,
		User:                  rmqUser,
		QueueName:             queueName,
		QueueCount:            queueCount,
		QuorumSize:            quorumSize,
		MsgSizeBytes:          msgSize,
		WarmupSeconds:         warmup,
		DurationSeconds:       duration,
		Publishers:            publishers,
		Consumers:             consumers,
		QueueMaxLength:        queueMaxLength,
		QueueOverflowStrategy: queueOverflowStrategy,
	}
	if err := rec.WriteConfig(benchmarkConfig); err != nil {
		log.Printf("Failed to write config file: %v", err)
	}

	rec.Start()
	defer rec.Stop()

	// Context controls the duration of the Run method of the experiment
	// The total runtime of the experiment is duration + warmup
	totalDuration := duration + warmup
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(totalDuration)*time.Second)
	defer cancel()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		fmt.Fprintln(os.Stderr, "\n[INTERRUPT] Gracefully cancelling benchmark...")
		log.Println("[INTERRUPT] Gracefully cancelling benchmark...")
		cancel()

		// Force exit if another interrupt is received
		<-sigChan
		fmt.Fprintln(os.Stderr, "\n[INTERRUPT] Forcing exit...")
		log.Println("[INTERRUPT] Forcing exit...")
		// Restore cursor visibility
		fmt.Fprint(os.Stderr, "\033[?25h")
		os.Exit(130)
	}()

	// Run the Experiment
	log.Printf("Running experiment for %d seconds (Warmup: %ds, Measure: %ds)...", totalDuration, warmup, duration)

	// Start progress bar
	mon := monitor.NewMonitor()
	mon.ConfigureDisplay(totalDuration, warmup)
	mon.Start()
	mon.StartDisplay()
	defer mon.Stop()

	summary, err := exp.Run(ctx, publishers, rec)

	// Finish display
	mon.FinishDisplay()
	fmt.Println("---------------------------------------------------")

	if err != nil {
		logAndPrint("Experiment failed: %v", err)
	}

	// Print Summary
	formattedSummary, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(formattedSummary))
	logAndPrint("---------------------------------------------------")
	logAndPrint("Results saved to: %s", rec.GetResultsPath())
	logAndPrint("Log file saved to: %s", logFile)
}
