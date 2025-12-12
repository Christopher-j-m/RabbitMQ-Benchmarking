// Parses command-line params, sets up logging, initializes the experiments and finally executes the benchmark.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"rmq-benchmark/experiments"
	"rmq-benchmark/metrics"
	"rmq-benchmark/rmq"
	"time"

	"github.com/spf13/cobra"
)

// Command-line parameters
var (
	rabbitURL      string
	mgmtURL        string
	user           string
	password       string
	queueName      string
	quorumSize     int
	msgSize        int
	warmup         int
	duration       int
	workers        int
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
	rootCmd.Flags().StringVar(&rabbitURL, "url", "amqp://guest:guest@localhost:5672/", "RabbitMQ AMQP URL")
	rootCmd.Flags().StringVar(&mgmtURL, "mgmt-url", "http://localhost:15672", "RabbitMQ Management URL")
	rootCmd.Flags().StringVar(&user, "user", "guest", "RabbitMQ User")
	rootCmd.Flags().StringVar(&password, "password", "guest", "RabbitMQ Password")
	rootCmd.Flags().StringVar(&queueName, "queue", "benchmark-queue", "Queue Name")
	rootCmd.Flags().IntVar(&quorumSize, "quorum-size", 3, "Quorum Queue Group Size")
	rootCmd.Flags().IntVar(&msgSize, "msg-size", 1024, "Message Size in Bytes")
	rootCmd.Flags().IntVar(&warmup, "warmup", 5, "Warmup duration in seconds")
	rootCmd.Flags().IntVar(&duration, "duration", 30, "Test duration in seconds")
	rootCmd.Flags().IntVar(&workers, "workers", 10, "Number of concurrent workers")
	rootCmd.Flags().IntVar(&consumers, "consumers", 0, "Number of concurrent consumers")
	rootCmd.Flags().StringVar(&experimentName, "experiment", "raft-tax", "Experiment to run: raft-tax, linear-capacity, proxy-tax") // TODO: Get it from registered experiments

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Main benchmark execution function
func runBenchmark(cmd *cobra.Command, args []string) {
	// Create logs directory and create log file
	logsDir := "logs"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		fmt.Printf("Failed to create logs directory: %v\n", err)
		os.Exit(1)
	}

	timestamp := time.Now().Format("20060102-150405")
	logFile := filepath.Join(logsDir, fmt.Sprintf("benchmark_%s_%s.log", experimentName, timestamp))
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Log to both file and console
	multiWriter := io.MultiWriter(os.Stdout, f)
	log.SetOutput(multiWriter)

	// Setup Controller for RabbitMQ Management API interactions
	ctrl := rmq.NewController(mgmtURL, user, password)

	// Experiment Configuration from command-line parameters
	exp, err := experiments.GetExperiment(experimentName)
	if err != nil {
		log.Fatalf("Failed to select experiment: %v", err)
	}
	config := experiments.Config{
		RabbitURL:       rabbitURL,
		ManagementURL:   mgmtURL,
		User:            user,
		Password:        password,
		QueueName:       queueName,
		QuorumSize:      quorumSize,
		MsgSize:         msgSize,
		WarmupSeconds:   warmup,
		DurationSeconds: duration,
		Consumers:       consumers,
	}

	// Setup Experiment with the config
	log.Printf("Setting up experiment: %s", exp.Name())
	if err := exp.Setup(config, ctrl); err != nil {
		log.Fatalf("Setup failed: %v", err)
	}
	defer exp.Teardown()

	// Create the Metrics Recorder & start recording
	// TODO: Make custom Recorders defineable per experiment (to keep being extensible	)
	rec, err := metrics.NewRecorder(exp.Name(), warmup)
	if err != nil {
		log.Fatalf("Failed to create recorder: %v", err)
	}
	rec.Start()
	defer rec.Stop()

	// Context controls the duration of the Run method
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(duration)*time.Second)
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
		log.Println("\n[FORCE EXIT] Forcing exit...")
		os.Exit(130)
	}()

	// Run the Experiment
	log.Printf("Running experiment for %d seconds...", duration)
	summary, err := exp.Run(ctx, workers, rec)
	if err != nil {
		log.Printf("Experiment finished with error: %v", err)
	}

	// Print Summary
	jsonSummary, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(jsonSummary))
}
