// Monitor Component
//
// Visualizes system metrics of the load-generator and progress during benchmark execution.
// By showing CPU (via top), Disk (via iostat), and Network usage (via ifstat) no additional
// windows need to be kept open to monitor system load and identify potential bottlenecks on the load generator.
//
// The progress bar shows elapsed time, percentage completed, and indicates whether the benchmark
// is in the warmup or measurement phase.
package monitor

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	colorReset   = "\033[0m"
	colorBold    = "\033[1m"
	colorDim     = "\033[2m"
	colorCyan    = "\033[36m"
	colorYellow  = "\033[33m"
	colorGreen   = "\033[32m"
	colorMagenta = "\033[35m"
	colorBlue    = "\033[34m"
	colorWhite   = "\033[97m"
)

// Phase contains the current benchmark phase (warmup or measurement)
type Phase int

const (
	PhaseWarmup Phase = iota
	PhaseMeasure
	PhaseFinished
)

// Current system metrics of the load generator
type SystemStats struct {
	CPUUsage  string
	DiskRead  string
	DiskWrite string
	NetIn     string
	NetOut    string
}

// Progress bar configuration vars
type DisplayConfig struct {
	TotalSeconds  int
	WarmupSeconds int
	BarWidth      int
}

type Monitor struct {
	currentStats SystemStats
	mutex        sync.RWMutex
	stopChan     chan struct{}

	// Display state
	config        DisplayConfig
	startTime     time.Time
	displayTicker *time.Ticker
	displayStop   chan struct{}
	linesRendered int
}

func NewMonitor() *Monitor {
	return &Monitor{
		currentStats: SystemStats{
			CPUUsage:  "  ─  ",
			DiskRead:  "─",
			DiskWrite: "─",
			NetIn:     "─",
			NetOut:    "─",
		},
		stopChan:    make(chan struct{}),
		displayStop: make(chan struct{}),
	}
}

// ConfigureDisplay sets up the progress display parameters
func (monitor *Monitor) ConfigureDisplay(totalSeconds, warmupSeconds int) {
	monitor.config = DisplayConfig{
		TotalSeconds:  totalSeconds,
		WarmupSeconds: warmupSeconds,
		BarWidth:      50,
	}
}

// Start the system stats fetching loop
func (monitor *Monitor) Start() {
	go monitor.updateLoop()
}

// Start the progress bar display loop
func (monitor *Monitor) StartDisplay() {
	monitor.startTime = time.Now()
	monitor.displayTicker = time.NewTicker(500 * time.Millisecond)

	// Hide cursor
	fmt.Fprint(os.Stderr, "\033[?25l")

	go monitor.displayLoop()
}

// Stop the progress bar and system stats loop
func (monitor *Monitor) Stop() {
	close(monitor.stopChan)

	if monitor.displayTicker != nil {
		monitor.displayTicker.Stop()
		close(monitor.displayStop)
	}

	// Show cursor again
	fmt.Fprint(os.Stderr, "\033[?25h")
}

// Clears the progress bar
func (monitor *Monitor) DisplayCleanup() {
	if monitor.displayTicker != nil {
		monitor.displayTicker.Stop()
	}

	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	// Set phase to finished
	elapsed := monitor.config.TotalSeconds
	stats := monitor.currentStats
	phase := PhaseFinished

	if monitor.linesRendered > 0 {
		fmt.Fprintf(os.Stderr, "\033[%dA", monitor.linesRendered)
	}

	output := monitor.buildDisplay(stats, elapsed, phase)
	fmt.Fprint(os.Stderr, output)

	// Print waiting message
	// => Especially the linear capacity exp. can take a while to shutdown publishers/consumers
	msg := "\nGracefully stopping the publishers/consumers (this may take a moment)..."
	fmt.Fprint(os.Stderr, msg)
	monitor.linesRendered = strings.Count(output, "\n") + 1
}

// Cleanup progress and print final state
func (monitor *Monitor) FinishDisplay() {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	if monitor.linesRendered > 0 {
		for i := 0; i < monitor.linesRendered; i++ {
			fmt.Fprint(os.Stderr, "\033[2K") // Clear line
			if i < monitor.linesRendered-1 {
				fmt.Fprint(os.Stderr, "\033[1A") // Move up
			}
		}
		fmt.Fprint(os.Stderr, "\r")
	}

	// Print completion message
	fmt.Fprintf(os.Stderr, "%s%s✓%s Benchmark finished\n",
		colorBold, colorGreen, colorReset)

	// Show cursor again
	fmt.Fprint(os.Stderr, "\033[?25h")
}

// Return current system statistics
func (monitor *Monitor) GetStats() SystemStats {
	monitor.mutex.RLock()
	defer monitor.mutex.RUnlock()
	return monitor.currentStats
}

// Show progress bar and update the progress display
func (monitor *Monitor) displayLoop() {
	monitor.render()
	for {
		select {
		case <-monitor.displayStop:
			return
		case <-monitor.displayTicker.C:
			monitor.render()
		}
	}
}

// Rneder the entire display of system stats and progress bar
func (monitor *Monitor) render() {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	elapsed := int(time.Since(monitor.startTime).Seconds())
	if elapsed > monitor.config.TotalSeconds {
		elapsed = monitor.config.TotalSeconds
	}

	stats := monitor.currentStats
	phase := PhaseWarmup
	if elapsed >= monitor.config.TotalSeconds {
		phase = PhaseFinished
	} else if elapsed > monitor.config.WarmupSeconds {
		phase = PhaseMeasure
	}

	// Clear previous render
	if monitor.linesRendered > 0 {
		// Move cursor up and clear lines
		fmt.Fprintf(os.Stderr, "\033[%dA", monitor.linesRendered)
	}

	// Build display
	output := monitor.buildDisplay(stats, elapsed, phase)

	// Print output
	fmt.Fprint(os.Stderr, output)

	// Count lines rendered (for next clear)
	monitor.linesRendered = strings.Count(output, "\n")
}

// Construct the complete display string of system stats and progress bar in two lines
func (monitor *Monitor) buildDisplay(stats SystemStats, elapsed int, phase Phase) string {
	var stringBuilder strings.Builder

	// Line 1: Stats line
	stringBuilder.WriteString(monitor.buildStatsLine(stats, phase))
	stringBuilder.WriteString("\n")

	// Line 2: Progress bar with time
	stringBuilder.WriteString(monitor.buildProgressLine(elapsed, phase))
	stringBuilder.WriteString("\n")

	return stringBuilder.String()
}

// Create the system stats display (first line)
func (monitor *Monitor) buildStatsLine(stats SystemStats, phase Phase) string {
	// Phase indicator
	var phaseStr string
	if phase == PhaseFinished {
		phaseStr = fmt.Sprintf("%s%s● FINISHED%s", colorBold, colorBlue, colorReset)
	} else if phase == PhaseWarmup {
		phaseStr = fmt.Sprintf("%s%s● WARMUP %s", colorBold, colorYellow, colorReset)
	} else {
		phaseStr = fmt.Sprintf("%s%s● MEASURE%s", colorBold, colorGreen, colorReset)
	}

	// Stats with labels
	cpuStr := fmt.Sprintf("%sCPU%s %s", colorCyan, colorReset, stats.CPUUsage)
	diskStr := fmt.Sprintf("%sDisk%s ↓%s ↑%s", colorMagenta, colorReset, stats.DiskRead, stats.DiskWrite)
	netStr := fmt.Sprintf("%sNet%s ↓%s ↑%s", colorBlue, colorReset, stats.NetIn, stats.NetOut)

	// Separator between stats
	sep := fmt.Sprintf("%s│%s", colorDim, colorReset)

	return fmt.Sprintf("%s %s %s %s %s %s %s", phaseStr, sep, cpuStr, sep, diskStr, sep, netStr)
}

// Create the progress bar with elapsed & total time
// on the right side of the progress bar (line 2)
func (monitor *Monitor) buildProgressLine(elapsed int, phase Phase) string {
	total := monitor.config.TotalSeconds
	warmup := monitor.config.WarmupSeconds
	barWidth := monitor.config.BarWidth

	// Calculate positions
	progress := float64(elapsed) / float64(total)
	warmupRatio := float64(warmup) / float64(total)
	warmupWidth := int(float64(barWidth) * warmupRatio)
	filledWidth := int(float64(barWidth) * progress)

	// Time display
	elapsedStr := formatDuration(elapsed)
	totalStr := formatDuration(total)
	timeStr := fmt.Sprintf("%s / %s", elapsedStr, totalStr)

	// Percentage
	percent := int(progress * 100)
	percentStr := fmt.Sprintf("%3d%%", percent)

	// Build progress bar
	var bar strings.Builder
	bar.WriteString(fmt.Sprintf("%s[%s", colorDim, colorReset))

	for i := 0; i < barWidth; i++ {
		isWarmupZone := i < warmupWidth
		isFilled := i < filledWidth
		isHead := i == filledWidth-1 && filledWidth > 0 && filledWidth < barWidth

		if isHead {
			if phase == PhaseWarmup {
				bar.WriteString(fmt.Sprintf("%s%s▶%s", colorBold, colorYellow, colorReset))
			} else {
				bar.WriteString(fmt.Sprintf("%s%s▶%s", colorBold, colorGreen, colorReset))
			}
		} else if isFilled {
			if isWarmupZone {
				bar.WriteString(fmt.Sprintf("%s━%s", colorYellow, colorReset))
			} else {
				bar.WriteString(fmt.Sprintf("%s━%s", colorGreen, colorReset))
			}
		} else {
			if i == warmupWidth && warmupWidth > 0 && warmupWidth < barWidth {
				// Separator between warmup and measure zones
				bar.WriteString(fmt.Sprintf("%s┃%s", colorWhite, colorReset))
			} else {
				bar.WriteString(fmt.Sprintf("%s─%s", colorDim, colorReset))
			}
		}
	}

	bar.WriteString(fmt.Sprintf("%s]%s", colorDim, colorReset))

	// Combine progress bar + percentage + time
	return fmt.Sprintf("%s %s%s%s  %s%s%s",
		bar.String(),
		colorBold, percentStr, colorReset,
		colorCyan, timeStr, colorReset)
}

// Regularly update system stats
func (monitor *Monitor) updateLoop() {
	// Update CPU/Disk every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	monitor.fetchCPU()
	monitor.fetchDisk()

	// Fetch network in separate goroutine
	go func() {
		for {
			select {
			case <-monitor.stopChan:
				return
			default:
				monitor.fetchNet()
			}
		}
	}()

	for {
		select {
		case <-monitor.stopChan:
			return
		case <-ticker.C:
			var waitGroup sync.WaitGroup
			waitGroup.Add(2)
			go func() { defer waitGroup.Done(); monitor.fetchCPU() }()
			go func() { defer waitGroup.Done(); monitor.fetchDisk() }()
			waitGroup.Wait()
		}
	}
}

// Fetch CPU usage via 'top -bn1'
// Adapted from https://stackoverflow.com/questions/9229333
func (monitor *Monitor) fetchCPU() {
	cmd := exec.Command("top", "-bn1")
	out, err := cmd.Output()
	if err != nil {
		monitor.updateStat("CPU", " err ")
		return
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "Cpu(s)") {
			regex := regexp.MustCompile(`([\d\.]+)\s*id`)
			matches := regex.FindStringSubmatch(line)
			if len(matches) > 1 {
				idleStr := matches[1]
				idle, err := strconv.ParseFloat(idleStr, 64)
				if err == nil {
					usage := 100.0 - idle
					monitor.updateStat("CPU", fmt.Sprintf("%5.1f%%", usage))
					return
				}
			}
		}
	}
	monitor.updateStat("CPU", "  ─  ")
}

// Fetch disk usage via 'iostat -d 1 2'
func (monitor *Monitor) fetchDisk() {
	cmd := exec.Command("sudo", "iotop", "-b", "-n", "1")
	out, err := cmd.Output()
	if err != nil {
		cmd = exec.Command("iotop", "-b", "-n", "1")
		out, err = cmd.Output()
		if err != nil {
			monitor.updateDisk("n/a", "n/a")
			return
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	if scanner.Scan() {
		line := scanner.Text()
		reRead := regexp.MustCompile(`Total DISK READ\s*:\s*([\d\.]+\s*[KMG]?B/s)`)
		reWrite := regexp.MustCompile(`Total DISK WRITE\s*:\s*([\d\.]+\s*[KMG]?B/s)`)

		matchRead := reRead.FindStringSubmatch(line)
		matchWrite := reWrite.FindStringSubmatch(line)

		r := "0B"
		w := "0B"
		if len(matchRead) > 1 {
			r = formatBytes(strings.TrimSpace(matchRead[1]))
		}
		if len(matchWrite) > 1 {
			w = formatBytes(strings.TrimSpace(matchWrite[1]))
		}

		monitor.updateDisk(r, w)
	}
}

// Fetch network usage via 'ifstat 1 1'
// Getting only the first interface's stats once (snapshot)
func (monitor *Monitor) fetchNet() {
	cmd := exec.Command("ifstat", "1", "1")
	out, err := cmd.Output()
	if err != nil {
		monitor.updateNet("err", "err")
		time.Sleep(1 * time.Second)
		return
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 3 {
		return
	}

	dataLine := lines[len(lines)-1]
	fields := strings.Fields(dataLine)

	if len(fields) >= 2 {
		inVal, err1 := strconv.ParseFloat(fields[0], 64)
		outVal, err2 := strconv.ParseFloat(fields[1], 64)

		if err1 == nil && err2 == nil {
			monitor.updateNet(formatNetSpeed(inVal), formatNetSpeed(outVal))
		}
	}
}

// Update a specific metric in the current stats
func (monitor *Monitor) updateStat(metric, val string) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()
	switch metric {
	case "CPU":
		monitor.currentStats.CPUUsage = val
	case "Disk":
		monitor.currentStats.DiskRead = val
		monitor.currentStats.DiskWrite = ""
	case "Net":
		monitor.currentStats.NetIn = val
		monitor.currentStats.NetOut = ""
	}
}

func (monitor *Monitor) updateDisk(read, write string) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()
	monitor.currentStats.DiskRead = read
	monitor.currentStats.DiskWrite = write
}

func (monitor *Monitor) updateNet(in, out string) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()
	monitor.currentStats.NetIn = in
	monitor.currentStats.NetOut = out
}

// Utility functions

// Formats seconds as MM:SS & HH:MM:SS
func formatDuration(seconds int) string {
	d := time.Duration(seconds) * time.Second
	h := int(d.Hours())
	min := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	if h > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, min, s)
	}
	return fmt.Sprintf("%02d:%02d", min, s)
}

// String cleanup of cmd terminal output
func formatBytes(val string) string {
	val = strings.ReplaceAll(val, " ", "")
	val = strings.ReplaceAll(val, "/s", "")
	return val
}

// Formats network speed values in KBps or MBps
func formatNetSpeed(kbps float64) string {
	if kbps >= 1024 {
		return fmt.Sprintf("%.1fMB", kbps/1024)
	}
	return fmt.Sprintf("%.0fKB", kbps)
}