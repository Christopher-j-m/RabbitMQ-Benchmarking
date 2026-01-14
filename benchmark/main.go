// Parses command-line params, sets up logging, initializes the experiments and finally executes the benchmark.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	amqpURL        string
	mgmtURL        string
	rmqUser        string
	rmqPassword    string
	queueName      string
	quorumSize     int
	msgSize        int
	warmup         int
	duration       int
	publishers     int
	consumers      int
	experimentName string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "rmq-benchmark",
		Short: "RabbitMQ Benchmarking Tool",
		Run:   runBenchmark,
	}

	// Define command-line flags and their default values
	rootCmd.Flags().StringVar(&amqpURL, "amq-url", "", "RabbitMQ AMQP URL (Required)")
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

	experimentsList := experiments.ListExperiments()
	helpText := fmt.Sprintf("Experiment to run: %v (Required)", experimentsList)
	rootCmd.Flags().StringVar(&experimentName, "experiment", "", helpText)

	// Mark flags as required and throw error if any are missing
	// We require users to explicitly specify these parameters
	rootCmd.MarkFlagRequired("amq-url")
	rootCmd.MarkFlagRequired("mgmt-url")
	rootCmd.MarkFlagRequired("rmq-user")
	rootCmd.MarkFlagRequired("rmq-password")
	rootCmd.MarkFlagRequired("experiment")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
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

	// Log to file only (to avoid interfering with progress bar)
	log.SetOutput(f)

	// Log CLI arguments
	// Omit Management URL connection string since it contains complete credentials (namely password)
	log.Println("---------------------------------------------------")
	log.Printf("Benchmark Parameters:")
	log.Printf("  Experiment: %s", experimentName)
	log.Printf("  Mgmt URL: %s", mgmtURL)
	log.Printf("  Rmq User: %s", rmqUser)
	log.Printf("  Queue Name: %s", queueName)
	log.Printf("  Quorum Size: %d", quorumSize)
	log.Printf("  Msg Size (bytes): %d", msgSize)
	log.Printf("  Warmup (seconds): %d", warmup)
	log.Printf("  Duration (seconds): %d", duration)
	log.Printf("  Publishers: %d", publishers)
	log.Printf("  Consumers: %d", consumers)
	log.Println("---------------------------------------------------")

	// Only print the CLI parameters to stdout
	// Warmup and duration are already shown in the progress bar hence they are omitted
	// => Further output goes to the log file to avoid interfering with progress bar
	fmt.Println("---------------------------------------------------")
	fmt.Printf("Experiment: %s\n", experimentName)
	fmt.Printf("Mgmt URL: %s\n", mgmtURL)
	fmt.Printf("Rmq User: %s\n", rmqUser)
	fmt.Printf("Queue Name: %s\n", queueName)
	fmt.Printf("Quorum Size: %d\n", quorumSize)
	fmt.Printf("Msg Size (bytes): %d\n", msgSize)
	fmt.Printf("Publishers: %d\n", publishers)
	fmt.Printf("Consumers: %d\n", consumers)
	fmt.Println("---------------------------------------------------")

	// Controller component contains common functionality for RabbitMQ node & Management API interactions
	ctrl := rmq.NewController(mgmtURL, rmqUser, rmqPassword)
	
	// Create specified experiment & pass parameters
	exp, err := experiments.GetExperiment(experimentName)
	if err != nil {
		log.Fatalf("Failed to select the specified experiment: %v", err)
	}

	config := experiments.Config{
		RabbitURL: amqpURL,
		ManagementURL: mgmtURL,
		User: rmqUser,
		Password: rmqPassword,
		QueueName: queueName,
		QuorumSize: quorumSize,
		MsgSize: msgSize,
		WarmupSeconds: warmup,
		DurationSeconds: duration,
		Publishers: publishers,
		Consumers: consumers,
	}
	if err := exp.Setup(config, ctrl); err != nil {
		log.Fatalf("Failed to configure the experiment: %v", err)
	}
	defer exp.Teardown()

	// Create the Metrics Recorder & start recording
	// TODO: Make custom Recorders defineable per experiment (to keep being extensible)
	rec, err := metrics.NewRecorder(experimentName, warmup)
	if err != nil {
		log.Fatalf("Failed to create recorder: %v", err)
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
		log.Println("\n[INTERRUPT] Gracefully cancelling benchmark...")
		cancel()
		
		// Force exit if another interrupt is received
		<-sigChan
		log.Println("\n[INTERRUPT] Forcing exit...")
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
		log.Printf("Experiment failed: %v", err)
		fmt.Printf("Experiment failed: %v\n", err)
	}

	// Print Summary
	formattedSummary, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(formattedSummary))
	fmt.Println("---------------------------------------------------")
	fmt.Printf("Log file saved to: %s\n", logFile)
}
