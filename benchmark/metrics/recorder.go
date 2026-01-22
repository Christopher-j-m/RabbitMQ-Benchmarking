// Implements the measurement of performance metrics that can be recorded during a benchmark experiment.
// Currently supports recording latencies and throughputs, and outputs results to CSV.
package metrics

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type LatencyBatch struct {
	Latencies []int64
}

// All CLI parameters corresponding to the current benchmark for traceability
type BenchmarkConfig struct {
	Experiment            string `json:"experiment"`
	StartTime             string `json:"start_time"`
	ManagementURL         string `json:"management_url"`
	User                  string `json:"user"`
	QueueName             string `json:"queue_name"`
	QueueCount            int    `json:"queue_count"`
	QuorumSize            int    `json:"quorum_size"`
	MsgSizeBytes          int    `json:"msg_size_bytes"`
	WarmupSeconds         int    `json:"warmup_seconds"`
	DurationSeconds       int    `json:"duration_seconds"`
	Publishers            int    `json:"publishers"`
	Consumers             int    `json:"consumers"`
	QueueMaxLength        int    `json:"queue_length,omitempty"`
	QueueOverflowStrategy string `json:"queue_overflow,omitempty"`
}

// Central component for capturing and aggregating all performance data.
// It uses a windowed approach to calculate metrics over short intervals and a global
// approach to determine final statistics after the warmup period.
// Uses channel-based batching to reduce mutex contention.
type Recorder struct {
	filename   string
	configFile string        // Path to the config file saved alongside the CSV
	warmup     time.Duration // Duration to skip before counting metrics (to reach steady state)
	startTime  time.Time     // Time when the benchmark started running
	windowSize time.Duration // Duration of a single measurement interval (e.g., 1 second)
	mutex      sync.Mutex    // Mutex to protect shared counters and histograms

	// Histograms
	// Used for interval-based reporting (written to CSV)
	windowHistogram *hdrhistogram.Histogram
	// Used for final summary (records only after warmup)
	globalHistogram *hdrhistogram.Histogram

	// Counters
	windowMsgCount int64
	globalMsgCount int64

	// Output control
	csvWriter *csv.Writer
	file      *os.File
	done      chan struct{}  // Channel to signal the background loop to stop
	waitGroup sync.WaitGroup // Waits for the background loop to finish

	// For summary stats
	throughputSamples []float64 // Stores throughput (messages/sec) for each interval after warmup
	errorCount        int64     // Total count of errors recorded after warmup

	// Batching channels
	latencyChan chan LatencyBatch
	batchWg     sync.WaitGroup
}

func NewRecorder(experimentName string, clusterSize int, warmupSeconds int) (*Recorder, error) {
	// Create results directory structure: results/<experiment-name>/<clusterSize>_<timestamp>/
	timestamp := time.Now().Format("20060102-150405")
	resultsDir := filepath.Join("results", experimentName, fmt.Sprintf("%d_%s", clusterSize, timestamp))

	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create results directory: %w", err)
	}

	// Create CSV and config files
	filename := filepath.Join(resultsDir, "results.csv")
	configFile := filepath.Join(resultsDir, "results.config")

	csvFile, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	csvWriter := csv.NewWriter(csvFile)
	header := []string{
		"elapsed_seconds",
		"interval_throughput",
		"interval_latency_mean_us",
		"interval_latency_p50_us",
		"interval_latency_p95_us",
		"interval_latency_p99_us",
		"interval_std_dev_us",
	}
	// Write the metrics to the CSV header
	if err := csvWriter.Write(header); err != nil {
		return nil, err
	}
	csvWriter.Flush()

	return &Recorder{
		filename:   filename,
		configFile: configFile,
		warmup:     time.Duration(warmupSeconds) * time.Second,
		windowSize: 1 * time.Second,
		// HdrHistograms setup: min=1us, max=10min (600,000,000us), significant figures=3
		windowHistogram:   hdrhistogram.New(1, 600000000, 3),
		globalHistogram:   hdrhistogram.New(1, 600000000, 3),
		csvWriter:         csvWriter,
		file:              csvFile,
		done:              make(chan struct{}),
		throughputSamples: make([]float64, 0),
		latencyChan:       make(chan LatencyBatch, 1000),
	}, nil
}

// Initiates the background loop that periodically flushes windowed metrics to CSV
func (recorder *Recorder) Start() {
	recorder.startTime = time.Now()
	recorder.waitGroup.Add(1)
	go recorder.loop()

	// Start batch processor routine
	recorder.batchWg.Add(1)
	go recorder.processBatches()
}

// Handles incoming latency batches
func (recorder *Recorder) processBatches() {
	defer recorder.batchWg.Done()
	for batch := range recorder.latencyChan {
		recorder.mutex.Lock()
		// Exclude measurements during warmup
		if time.Since(recorder.startTime) > recorder.warmup {
			for _, us := range batch.Latencies {
				_ = recorder.windowHistogram.RecordValue(us)
				recorder.windowMsgCount++
				_ = recorder.globalHistogram.RecordValue(us)
				recorder.globalMsgCount++
			}
		}
		recorder.mutex.Unlock()
	}
}

// RecordLatencyBatch submits a batch of latency measurements (in microseconds) to the recorder.
// This is more efficient than calling RecordLatency for each measurement.
func (recorder *Recorder) RecordLatencyBatch(latenciesUs []int64) {
	if len(latenciesUs) == 0 {
		return
	}
	// Make a copy to avoid data races
	batch := make([]int64, len(latenciesUs))
	copy(batch, latenciesUs)
	select {
	case recorder.latencyChan <- LatencyBatch{Latencies: batch}:
	default:
		// Channel full, fall back to direct recording
		recorder.mutex.Lock()
		// Exclude measurements during warmup
		if time.Since(recorder.startTime) > recorder.warmup {
			for _, us := range latenciesUs {
				_ = recorder.windowHistogram.RecordValue(us)
				recorder.windowMsgCount++
				_ = recorder.globalHistogram.RecordValue(us)
				recorder.globalMsgCount++
			}
		}
		recorder.mutex.Unlock()
	}
}

// Increments the error counter, but only after the warmup period.
func (recorder *Recorder) RecordError() {
	if time.Since(recorder.startTime) > recorder.warmup {
		atomic.AddInt64(&recorder.errorCount, 1)
	}
}

// The background timer that triggers periodically to write the current windows metrics to CSV
func (recorder *Recorder) loop() {
	defer recorder.waitGroup.Done()
	ticker := time.NewTicker(recorder.windowSize)
	defer ticker.Stop()

	for {
		select {
		case <-recorder.done:
			return
		case t := <-ticker.C:
			recorder.flushWindow(t)
		}
	}
}

// Snapshots the current intervals metrics, writes them to CSV and resets the window
func (recorder *Recorder) flushWindow(flushTime time.Time) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()

	elapsed := flushTime.Sub(recorder.startTime).Seconds()

	// Snapshot metrics
	count := recorder.windowMsgCount
	mean := recorder.windowHistogram.Mean()
	p50 := recorder.windowHistogram.ValueAtQuantile(50)
	p95 := recorder.windowHistogram.ValueAtQuantile(95)
	p99 := recorder.windowHistogram.ValueAtQuantile(99)
	stdDev := recorder.windowHistogram.StdDev()

	// Write to CSV only if warmup period has passed
	if elapsed > recorder.warmup.Seconds() {
		record := []string{
			fmt.Sprintf("%.2f", elapsed),
			fmt.Sprintf("%d", count),
			fmt.Sprintf("%.2f", mean),
			fmt.Sprintf("%d", p50),
			fmt.Sprintf("%d", p95),
			fmt.Sprintf("%d", p99),
			fmt.Sprintf("%.2f", stdDev),
		}
		_ = recorder.csvWriter.Write(record)
		recorder.csvWriter.Flush()

		// Store throughput for summary
		recorder.throughputSamples = append(recorder.throughputSamples, float64(count))
	}

	// Reset window histogram
	recorder.windowHistogram.Reset()
	recorder.windowMsgCount = 0
}

// Stops the background loop and closes the CSV file
func (recorder *Recorder) Stop() {
	close(recorder.done)
	recorder.waitGroup.Wait()

	close(recorder.latencyChan)
	recorder.batchWg.Wait()

	_ = recorder.file.Close()
}

// The metrics that will be printed out at the end of an experiment for a general overview of the experiment results
type Summary struct {
	MeanThroughput   float64 `json:"mean_throughput"`
	StdDevThroughput float64 `json:"std_dev_throughput"`
	GlobalP99Latency int64   `json:"global_p99_latency_us"`
	TotalErrorCount  int64   `json:"total_error_count"`
}

// Save the benchmark cli params to .config file alongside the results CSV.
// This allows tracing back what parameters were used for the corresponding results.
func (recorder *Recorder) WriteConfig(config BenchmarkConfig) error {
	f, err := os.Create(recorder.configFile)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}
	defer func() { _ = f.Close() }()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(config); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}
	return nil
}

// Return the path to the results CSV file
func (recorder *Recorder) GetResultsPath() string {
	return recorder.filename
}

// Return the path to the config file
func (recorder *Recorder) GetConfigPath() string {
	return recorder.configFile
}

// Calculates the summary metrics from the collected data (excluding warmup).
// The global histogram is used to get the P99 latency.
func (recorder *Recorder) GetSummary() Summary {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()

	var sum float64
	for _, value := range recorder.throughputSamples {
		sum += value
	}

	mean := 0.0
	stdDev := 0.0

	if len(recorder.throughputSamples) > 0 {
		// Calculate mean throughput
		mean = sum / float64(len(recorder.throughputSamples))

		var varianceSum float64
		for _, value := range recorder.throughputSamples {
			varianceSum += (value - mean) * (value - mean)
		}

		// Calculate standard deviation of throughput
		if len(recorder.throughputSamples) > 1 {
			stdDev = math.Sqrt(varianceSum / float64(len(recorder.throughputSamples)-1))
		}
	}

	return Summary{
		MeanThroughput:   mean,
		StdDevThroughput: stdDev,
		GlobalP99Latency: recorder.globalHistogram.ValueAtQuantile(99),
		TotalErrorCount:  atomic.LoadInt64(&recorder.errorCount),
	}
}

// Format the benchmark summary when printed to console
func (s Summary) String() string {
	var output string

	if s.MeanThroughput > 0 {
		output += fmt.Sprintf("%-22s %.2f msg/s\n", "Mean Throughput:", s.MeanThroughput)
		output += fmt.Sprintf("%-22s %.2f msg/s\n", "StdDev Throughput:", s.StdDevThroughput)
	}
	if s.GlobalP99Latency > 0 {
		output += fmt.Sprintf("%-22s %d µs\n", "Global P99 Latency:", s.GlobalP99Latency)
	}

	output += fmt.Sprintf("%-22s %d", "Recording Errors:", s.TotalErrorCount)
	return output
}
