package services

import (
	"fmt"
	"github.com/syleron/pulseha/internal/pulseha/models"
	"sync"
)

type ClusterManager struct {
	nodes map[string]*models.Node

	sync.Mutex
}

// NewClusterManager creates a new ClusterManager
func NewClusterManager() *ClusterManager {
	return &ClusterManager{
		nodes: make(map[string]*models.Node),
	}
}

// AddNode adds a new node to the cluster
func (c *ClusterManager) AddNode(node *models.Node) error {
	c.Lock()
	defer c.Unlock()

	// Check if the node already exists in the cluster
	if _, exists := c.nodes[node.Hostname]; exists {
		return fmt.Errorf("node with hostname %s already exists", node.Hostname)
	}

	// Add the node if it does not exist
	c.nodes[node.Hostname] = node
	return nil
}

// RemoveNode removes a node from the cluster
func (c *ClusterManager) RemoveNode(nodeID string) {
	c.Lock()
	defer c.Unlock()
	delete(c.nodes, nodeID)
}

// JoinCluster joins the cluster
func (c *ClusterManager) JoinCluster() {}

// LeaveCluster leaves the cluster
func (c *ClusterManager) LeaveCluster() {}

// MakeNodeActive makes the node active
func (c *ClusterManager) MakeNodeActive() {}

// MakeNodePassive makes the node passive
func (c *ClusterManager) MakeNodePassive() {}

func (c *ClusterManager) GetNode(nodeID string) *models.Node {
	c.Lock()
	defer c.Unlock()
	return c.nodes[nodeID]
}

func (c *ClusterManager) GetNodes() map[string]*models.Node {
	c.Lock()
	defer c.Unlock()
	return c.nodes
}

func (c *ClusterManager) SyncConfig() {}
