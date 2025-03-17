package ipam

import (
	"sync"
)

// IPDistributionStrategy defines how IPs are distributed across nodes
type IPDistributionStrategy int

const (
	// StrategyActivePassive traditional active-passive setup
	StrategyActivePassive IPDistributionStrategy = iota
	// StrategyActiveActive distributed active-active setup
	StrategyActiveActive
	// StrategyWeighted weighted distribution based on node capacity
	StrategyWeighted
)

// Balancer handles IP distribution across nodes
type Balancer struct {
	sync.RWMutex
	strategy IPDistributionStrategy
	// Map of IP to node assignments
	assignments map[string]string
	// Node weights for weighted distribution
	weights map[string]int
}

// NewBalancer creates a new IP balancer
func NewBalancer(strategy IPDistributionStrategy) *Balancer {
	return &Balancer{
		strategy:    strategy,
		assignments: make(map[string]string),
		weights:     make(map[string]int),
	}
}

// AssignIP assigns an IP to a specific node
func (b *Balancer) AssignIP(ip string, nodeID string) {
	b.Lock()
	defer b.Unlock()
	b.assignments[ip] = nodeID
}

// UnassignIP removes an IP assignment
func (b *Balancer) UnassignIP(ip string) {
	b.Lock()
	defer b.Unlock()
	delete(b.assignments, ip)
}

// GetIPAssignments returns current IP assignments
func (b *Balancer) GetIPAssignments() map[string]string {
	b.RLock()
	defer b.RUnlock()
	assignments := make(map[string]string)
	for ip, node := range b.assignments {
		assignments[ip] = node
	}
	return assignments
}

// Rebalance redistributes IPs across available nodes
func (b *Balancer) Rebalance(availableNodes []string, ips []string) map[string][]string {
	b.Lock()
	defer b.Unlock()

	nodeAssignments := make(map[string][]string)

	switch b.strategy {
	case StrategyActiveActive:
		// Distribute IPs evenly across all available nodes
		for i, ip := range ips {
			nodeIndex := i % len(availableNodes)
			nodeID := availableNodes[nodeIndex]
			nodeAssignments[nodeID] = append(nodeAssignments[nodeID], ip)
			b.assignments[ip] = nodeID
		}
	case StrategyWeighted:
		// Distribute IPs based on node weights
		totalWeight := 0
		for _, node := range availableNodes {
			totalWeight += b.weights[node]
		}

		ipIndex := 0
		for _, node := range availableNodes {
			nodeWeight := b.weights[node]
			ipCount := (nodeWeight * len(ips)) / totalWeight

			for i := 0; i < ipCount && ipIndex < len(ips); i++ {
				nodeAssignments[node] = append(nodeAssignments[node], ips[ipIndex])
				b.assignments[ips[ipIndex]] = node
				ipIndex++
			}
		}
	}

	return nodeAssignments
}

// SetNodeWeight sets the weight for weighted distribution
func (b *Balancer) SetNodeWeight(nodeID string, weight int) {
	b.Lock()
	defer b.Unlock()
	b.weights[nodeID] = weight
}
