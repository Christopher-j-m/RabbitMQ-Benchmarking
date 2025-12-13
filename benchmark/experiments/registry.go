package experiments

import (
	"fmt"
	"sort"
)

// Experiment Names
const (
	ExperimentRaftLatency    = "raft-latency"
	ExperimentLinearCapacity = "linear-capacity"
	ExperimentProxyLatency   = "proxy-latency"
)

type Factory func() Experiment

// Mapping of experiment names to their constructors.
var registry = map[string]Factory{
	ExperimentRaftLatency:    func() Experiment { return &RaftLatency{} },
	ExperimentLinearCapacity: func() Experiment { return &LinearCapacity{} },
	ExperimentProxyLatency:   func() Experiment { return &ProxyLatency{} },
}

// Returns a new instance of the specified experiment.
func GetExperiment(name string) (Experiment, error) {
	if factory, ok := registry[name]; ok {
		return factory(), nil
	}
	return nil, fmt.Errorf("unknown experiment: %s", name)
}

// Returns a list of all registered experiment names.
func ListExperiments() []string {
	keys := make([]string, 0, len(registry))
	for k := range registry {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
