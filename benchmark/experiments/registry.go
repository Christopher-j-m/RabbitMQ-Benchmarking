package experiments

import "fmt"

// Map of available experiments based on their unique names.
// Every experiment must be registered here to be discoverable by the benchmark.
func GetExperiment(name string) (Experiment, error) {
	switch name {
	case "raft-tax":
		return &RaftTax{}, nil
	case "linear-capacity":
		return &LinearCapacity{}, nil
	case "proxy-tax":
		return &ProxyTax{}, nil
	default:
		return nil, fmt.Errorf("unknown experiment: %s", name)
	}
}
