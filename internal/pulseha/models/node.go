package models

import (
	"time"
)

// Node represents a node in the cluster.
type Node struct {
	// The hostname of the repented node
	Hostname string

	// The status of the node
	Status interface{}

	// The last time a health check was received
	LastHCResponse time.Time

	// The latency between the active and the current passive member
	ClusterLatency string

	// Determines if the health check is being made.
	IsHealthCheckInProgress bool

	// Used to determine which node to fail over to
	FailoverScore int
}
