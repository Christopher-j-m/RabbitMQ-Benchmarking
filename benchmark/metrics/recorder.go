// Implements the measurement of performance metrics that can be recorded during a benchmark experiment.
// Currently supports recording latencies and throughputs, and outputs results to CSV.
package metrics

import (
	"encoding/csv"
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

// Central component for capturing and aggregating all performance data.
// It uses a windowed approach to calculate metrics over short intervals and a global
// approach to determine final statistics after the warmup period.
// Uses channel-based batching to reduce mutex contention.
type Recorder struct {
	filename   string
	warmup     time.Duration // Duration to skip before counting metrics (to reach steady state)
	startTime  time.Time     // Time when the benchmark started running
	windowSize time.Duration // Duration of a single measurement interval (e.g., 1 second)
	mu         sync.Mutex    // Mutex to protect shared counters and histograms

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
	wg        sync.WaitGroup // Waits for the background loop to finish

	// For summary stats
	throughputSamples []float64 // Stores throughput (messages/sec) for each interval after warmup
	errorCount        int64     // Total count of errors recorded after warmup

	// Batching channels
	latencyChan chan LatencyBatch
	batchWg     sync.WaitGroup
}

func NewRecorder(experimentName string, warmupSeconds int) (*Recorder, error) {
	// Create results directory if it doesn't exist
	resultsDir := "results"
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create results directory: %w", err)
	}

	// Create CSV file with timestamped name
	timestamp := time.Now().Format("20060102-150405")
	filename := filepath.Join(resultsDir, fmt.Sprintf("results_%s_%s.csv", experimentName, timestamp))

	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	// CSV Writer
	w := csv.NewWriter(f)
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
	if err := w.Write(header); err != nil {
		return nil, err
	}
	w.Flush()

	return &Recorder{
		filename:   filename,
		warmup:     time.Duration(warmupSeconds) * time.Second,
		windowSize: 1 * time.Second,
		// HdrHistograms setup: min=1us, max=10min (600,000,000us), significant figures=3
		windowHistogram:   hdrhistogram.New(1, 600000000, 3),
		globalHistogram:   hdrhistogram.New(1, 600000000, 3),
		csvWriter:         w,
		file:              f,
		done:              make(chan struct{}),
		throughputSamples: make([]float64, 0),
		latencyChan:       make(chan LatencyBatch, 1000),
	}, nil
}

// Initiates the background loop that periodically flushes windowed metrics to CSV
func (r *Recorder) Start() {
	r.startTime = time.Now()
	r.wg.Add(1)
	go r.loop()

	// Start batch processor routine
	r.batchWg.Add(1)
	go r.processBatches()
}

// Handles incoming latency batches
func (r *Recorder) processBatches() {
	defer r.batchWg.Done()
	for batch := range r.latencyChan {
		r.mu.Lock()
		// Exclude measurements during warmup
		if time.Since(r.startTime) > r.warmup {
			for _, us := range batch.Latencies {
				r.windowHistogram.RecordValue(us)
				r.windowMsgCount++
				r.globalHistogram.RecordValue(us)
				r.globalMsgCount++
			}
		}
		r.mu.Unlock()
	}
}

// RecordLatencyBatch submits a batch of latency measurements (in microseconds) to the recorder.
// This is more efficient than calling RecordLatency for each measurement.
func (r *Recorder) RecordLatencyBatch(latenciesUs []int64) {
	if len(latenciesUs) == 0 {
		return
	}
	// Make a copy to avoid data races
	batch := make([]int64, len(latenciesUs))
	copy(batch, latenciesUs)
	select {
	case r.latencyChan <- LatencyBatch{Latencies: batch}:
	default:
		// Channel full, fall back to direct recording
		r.mu.Lock()
		// Exclude measurements during warmup
		if time.Since(r.startTime) > r.warmup {
			for _, us := range latenciesUs {
				r.windowHistogram.RecordValue(us)
				r.windowMsgCount++
				r.globalHistogram.RecordValue(us)
				r.globalMsgCount++
			}
		}
		r.mu.Unlock()
	}
}

// Increments the error counter, but only after the warmup period.
func (r *Recorder) RecordError() {
	if time.Since(r.startTime) > r.warmup {
		atomic.AddInt64(&r.errorCount, 1)
	}
}

// The background timer that triggers periodically to write the current windows metrics to CSV
func (r *Recorder) loop() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.windowSize)
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return
		case t := <-ticker.C:
			r.flushWindow(t)
		}
	}
}

// Snapshots the current intervals metrics, writes them to CSV and resets the window
func (r *Recorder) flushWindow(t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	elapsed := t.Sub(r.startTime).Seconds()

	// Snapshot metrics
	count := r.windowMsgCount
	mean := r.windowHistogram.Mean()
	p50 := r.windowHistogram.ValueAtQuantile(50)
	p95 := r.windowHistogram.ValueAtQuantile(95)
	p99 := r.windowHistogram.ValueAtQuantile(99)
	stdDev := r.windowHistogram.StdDev()

	// Write to CSV only if warmup period has passed
	if elapsed > r.warmup.Seconds() {
		record := []string{
			fmt.Sprintf("%.2f", elapsed),
			fmt.Sprintf("%d", count),
			fmt.Sprintf("%.2f", mean),
			fmt.Sprintf("%d", p50),
			fmt.Sprintf("%d", p95),
			fmt.Sprintf("%d", p99),
			fmt.Sprintf("%.2f", stdDev),
		}
		r.csvWriter.Write(record)
		r.csvWriter.Flush()

		// Store throughput for summary
		r.throughputSamples = append(r.throughputSamples, float64(count))
	}

	// Reset window histogram
	r.windowHistogram.Reset()
	r.windowMsgCount = 0
}

// Stops the background loop and closes the CSV file
func (r *Recorder) Stop() {
	close(r.done)
	r.wg.Wait()

	close(r.latencyChan)
	r.batchWg.Wait()

	r.file.Close()
}

// The metrics that will be printed out at the end of an experiment for a general overview of the experiment results
type Summary struct {
	MeanThroughput   float64 `json:"mean_throughput"`
	StdDevThroughput float64 `json:"std_dev_throughput"`
	GlobalP99Latency int64   `json:"global_p99_latency_us"`
	TotalErrorCount  int64   `json:"total_error_count"`
}

// Calculates the summary metrics from the collected data (excluding warmup).
// The global histogram is used to get the P99 latency.
func (r *Recorder) GetSummary() Summary {
	r.mu.Lock()
	defer r.mu.Unlock()

	var sum float64
	for _, v := range r.throughputSamples {
		sum += v
	}

	mean := 0.0
	stdDev := 0.0

	if len(r.throughputSamples) > 0 {
		// Calculate mean throughput
		mean = sum / float64(len(r.throughputSamples))

		var varianceSum float64
		for _, v := range r.throughputSamples {
			varianceSum += (v - mean) * (v - mean)
		}

		// Calculate standard deviation of throughput
		if len(r.throughputSamples) > 1 {
			stdDev = math.Sqrt(varianceSum / float64(len(r.throughputSamples)-1))
		}
	}

	return Summary{
		MeanThroughput:   mean,
		StdDevThroughput: stdDev,
		GlobalP99Latency: r.globalHistogram.ValueAtQuantile(99),
		TotalErrorCount:  atomic.LoadInt64(&r.errorCount),
	}
}
