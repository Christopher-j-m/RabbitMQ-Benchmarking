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
	mu           sync.RWMutex
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
func (m *Monitor) ConfigureDisplay(totalSeconds, warmupSeconds int) {
	m.config = DisplayConfig{
		TotalSeconds:  totalSeconds,
		WarmupSeconds: warmupSeconds,
		BarWidth:      50,
	}
}

// Start the system stats fetching loop
func (m *Monitor) Start() {
	go m.updateLoop()
}

// Start the progress bar display loop
func (m *Monitor) StartDisplay() {
	m.startTime = time.Now()
	m.displayTicker = time.NewTicker(500 * time.Millisecond)

	// Hide cursor
	fmt.Fprint(os.Stderr, "\033[?25l")

	go m.displayLoop()
}

// Stop the progress bar and system stats loop
func (m *Monitor) Stop() {
	close(m.stopChan)

	if m.displayTicker != nil {
		m.displayTicker.Stop()
		close(m.displayStop)
	}

	// Show cursor again
	fmt.Fprint(os.Stderr, "\033[?25h")
}

// Clears the progress bar
func (m *Monitor) DisplayCleanup() {
	if m.displayTicker != nil {
		m.displayTicker.Stop()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Set phase to finished
	elapsed := m.config.TotalSeconds
	stats := m.currentStats
	phase := PhaseFinished

	if m.linesRendered > 0 {
		fmt.Fprintf(os.Stderr, "\033[%dA", m.linesRendered)
	}

	output := m.buildDisplay(stats, elapsed, phase)
	fmt.Fprint(os.Stderr, output)

	// Print waiting message
	// => Especially the linear capacity exp. can take a while to shutdown publishers/consumers
	msg := "\nGracefully stopping the publishers/consumers (this may take a moment)..."
	fmt.Fprint(os.Stderr, msg)
	m.linesRendered = strings.Count(output, "\n") + 1
}

// Cleanup progress and print final state
func (m *Monitor) FinishDisplay() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.linesRendered > 0 {
		for i := 0; i < m.linesRendered; i++ {
			fmt.Fprint(os.Stderr, "\033[2K") // Clear line
			if i < m.linesRendered-1 {
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
func (m *Monitor) GetStats() SystemStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentStats
}

// Show progress bar and update the progress display
func (m *Monitor) displayLoop() {
	m.render()
	for {
		select {
		case <-m.displayStop:
			return
		case <-m.displayTicker.C:
			m.render()
		}
	}
}

// Rneder the entire display of system stats and progress bar
func (m *Monitor) render() {
	m.mu.Lock()
	defer m.mu.Unlock()

	elapsed := int(time.Since(m.startTime).Seconds())
	if elapsed > m.config.TotalSeconds {
		elapsed = m.config.TotalSeconds
	}

	stats := m.currentStats
	phase := PhaseWarmup
	if elapsed >= m.config.TotalSeconds {
		phase = PhaseFinished
	} else if elapsed > m.config.WarmupSeconds {
		phase = PhaseMeasure
	}

	// Clear previous render
	if m.linesRendered > 0 {
		// Move cursor up and clear lines
		fmt.Fprintf(os.Stderr, "\033[%dA", m.linesRendered)
	}

	// Build display
	output := m.buildDisplay(stats, elapsed, phase)

	// Print output
	fmt.Fprint(os.Stderr, output)

	// Count lines rendered (for next clear)
	m.linesRendered = strings.Count(output, "\n")
}

// Construct the complete display string of system stats and progress bar in two lines
func (m *Monitor) buildDisplay(stats SystemStats, elapsed int, phase Phase) string {
	var sb strings.Builder

	// Line 1: Stats line
	sb.WriteString(m.buildStatsLine(stats, phase))
	sb.WriteString("\n")

	// Line 2: Progress bar with time
	sb.WriteString(m.buildProgressLine(elapsed, phase))
	sb.WriteString("\n")

	return sb.String()
}

// Create the system stats display (first line)
func (m *Monitor) buildStatsLine(stats SystemStats, phase Phase) string {
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
func (m *Monitor) buildProgressLine(elapsed int, phase Phase) string {
	total := m.config.TotalSeconds
	warmup := m.config.WarmupSeconds
	barWidth := m.config.BarWidth

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
func (m *Monitor) updateLoop() {
	// Update CPU/Disk every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	m.fetchCPU()
	m.fetchDisk()

	// Fetch network in separate goroutine
	go func() {
		for {
			select {
			case <-m.stopChan:
				return
			default:
				m.fetchNet()
			}
		}
	}()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); m.fetchCPU() }()
			go func() { defer wg.Done(); m.fetchDisk() }()
			wg.Wait()
		}
	}
}

// Fetch CPU usage via 'top -bn1'
// Adapted from https://stackoverflow.com/questions/9229333
func (m *Monitor) fetchCPU() {
	cmd := exec.Command("top", "-bn1")
	out, err := cmd.Output()
	if err != nil {
		m.updateStat("CPU", " err ")
		return
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "Cpu(s)") {
			re := regexp.MustCompile(`([\d\.]+)\s*id`)
			matches := re.FindStringSubmatch(line)
			if len(matches) > 1 {
				idleStr := matches[1]
				idle, err := strconv.ParseFloat(idleStr, 64)
				if err == nil {
					usage := 100.0 - idle
					m.updateStat("CPU", fmt.Sprintf("%5.1f%%", usage))
					return
				}
			}
		}
	}
	m.updateStat("CPU", "  ─  ")
}

// Fetch disk usage via 'iostat -d 1 2'
func (m *Monitor) fetchDisk() {
	cmd := exec.Command("sudo", "iotop", "-b", "-n", "1")
	out, err := cmd.Output()
	if err != nil {
		cmd = exec.Command("iotop", "-b", "-n", "1")
		out, err = cmd.Output()
		if err != nil {
			m.updateDisk("n/a", "n/a")
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

		m.updateDisk(r, w)
	}
}

// Fetch network usage via 'ifstat 1 1'
// Getting only the first interface's stats once (snapshot)
func (m *Monitor) fetchNet() {
	cmd := exec.Command("ifstat", "1", "1")
	out, err := cmd.Output()
	if err != nil {
		m.updateNet("err", "err")
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
			m.updateNet(formatNetSpeed(inVal), formatNetSpeed(outVal))
		}
	}
}

// Update a specific metric in the current stats
func (m *Monitor) updateStat(metric, val string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch metric {
	case "CPU":
		m.currentStats.CPUUsage = val
	case "Disk":
		m.currentStats.DiskRead = val
		m.currentStats.DiskWrite = ""
	case "Net":
		m.currentStats.NetIn = val
		m.currentStats.NetOut = ""
	}
}

func (m *Monitor) updateDisk(read, write string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentStats.DiskRead = read
	m.currentStats.DiskWrite = write
}

func (m *Monitor) updateNet(in, out string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentStats.NetIn = in
	m.currentStats.NetOut = out
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
