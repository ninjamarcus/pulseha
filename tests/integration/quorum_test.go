package integration

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/syleron/pulseha/tests/testutils"
)

func TestQuorumVoting(t *testing.T) {
	// Set test mode to skip hostname validation
	os.Setenv("PULSEHA_TEST", "true")
	defer os.Unsetenv("PULSEHA_TEST")

	cluster := testutils.NewTestCluster()
	defer cluster.Cleanup()

	// Add and start three nodes
	node1, err := cluster.AddNode("node1")
	require.NoError(t, err, "Failed to add first node")
	err = node1.Start()
	require.NoError(t, err, "Failed to start first node")
	time.Sleep(500 * time.Millisecond)

	node2, err := cluster.AddNode("node2")
	require.NoError(t, err, "Failed to add second node")
	err = node2.Start()
	require.NoError(t, err, "Failed to start second node")
	time.Sleep(500 * time.Millisecond)

	node3, err := cluster.AddNode("node3")
	require.NoError(t, err, "Failed to add third node")
	err = node3.Start()
	require.NoError(t, err, "Failed to start third node")
	time.Sleep(500 * time.Millisecond)

	// Join nodes to form a cluster
	err = node2.Join(node1)
	require.NoError(t, err, "Failed to join second node to cluster")
	time.Sleep(500 * time.Millisecond)

	err = node3.Join(node1)
	require.NoError(t, err, "Failed to join third node to cluster")
	time.Sleep(1 * time.Second)

	// Enable quorum voting on node1
	node1.Config.Pulse.QuorumEnabled = true
	node1.Config.Pulse.QuorumMinNodes = 2
	node1.Config.Pulse.QuorumMajorityMode = true
	err = node1.Config.Save()
	require.NoError(t, err, "Failed to save config with quorum enabled")

	// Sync config to other nodes
	err = node1.SyncConfigWithNode(node2)
	require.NoError(t, err, "Failed to sync config to node2")
	err = node1.SyncConfigWithNode(node3)
	require.NoError(t, err, "Failed to sync config to node3")

	// Add a longer delay to ensure config is properly synced
	time.Sleep(2 * time.Second)

	// Reload configs from disk to ensure we have the latest values
	err = node1.Config.Load()
	require.NoError(t, err, "Failed to reload config for node1")
	err = node2.Config.Load()
	require.NoError(t, err, "Failed to reload config for node2")
	err = node3.Config.Load()
	require.NoError(t, err, "Failed to reload config for node3")

	// Verify quorum settings on all nodes
	require.True(t, node1.Config.Pulse.QuorumEnabled, "Quorum should be enabled on node1")
	require.True(t, node2.Config.Pulse.QuorumEnabled, "Quorum should be enabled on node2")
	require.True(t, node3.Config.Pulse.QuorumEnabled, "Quorum should be enabled on node3")

	// Test node status changes with quorum
	// This is a basic test - in a real scenario, we would need to mock the quorum voting process
	// or use the actual RPC calls to test the full voting flow

	// For now, we'll just verify that the quorum configuration is properly synchronized
	require.Equal(t, 2, node1.Config.Pulse.QuorumMinNodes, "QuorumMinNodes should be 2 on node1")
	require.Equal(t, 2, node2.Config.Pulse.QuorumMinNodes, "QuorumMinNodes should be 2 on node2")
	require.Equal(t, 2, node3.Config.Pulse.QuorumMinNodes, "QuorumMinNodes should be 2 on node3")

	require.True(t, node1.Config.Pulse.QuorumMajorityMode, "QuorumMajorityMode should be true on node1")
	require.True(t, node2.Config.Pulse.QuorumMajorityMode, "QuorumMajorityMode should be true on node2")
	require.True(t, node3.Config.Pulse.QuorumMajorityMode, "QuorumMajorityMode should be true on node3")
}

func TestQuorumMajorityCalculation(t *testing.T) {
	// Set test mode to skip hostname validation
	os.Setenv("PULSEHA_TEST", "true")
	defer os.Unsetenv("PULSEHA_TEST")

	cluster := testutils.NewTestCluster()
	defer cluster.Cleanup()

	// Add and start a single node
	node1, err := cluster.AddNode("node1")
	require.NoError(t, err, "Failed to add node")

	// Configure quorum with majority mode
	node1.Config.Pulse.QuorumEnabled = true
	node1.Config.Pulse.QuorumMajorityMode = true

	// Test with different node counts
	testCases := []struct {
		nodeCount int
		expected  int // Expected minimum votes for quorum
	}{
		{1, 1}, // Single node: quorum is just that node
		{2, 2}, // Two nodes: need both for majority
		{3, 2}, // Three nodes: need 2 for majority
		{4, 3}, // Four nodes: need 3 for majority
		{5, 3}, // Five nodes: need 3 for majority
		{6, 4}, // Six nodes: need 4 for majority
	}

	for _, tc := range testCases {
		// Create the quorum manager
		quorumMgr := testutils.NewTestQuorumManager(node1.Config, node1.Logger)
		quorumMgr.UpdateNodeCount(tc.nodeCount)

		// Calculate the minimum votes needed for quorum
		minVotes := quorumMgr.CalculateQuorumMinimum(node1.Config, tc.nodeCount)

		require.Equal(t, tc.expected, minVotes,
			"For %d nodes, expected minimum votes for quorum to be %d, got %d",
			tc.nodeCount, tc.expected, minVotes)
	}
}

func TestQuorumInActivePassiveMode(t *testing.T) {
	// Create a new test cluster
	cluster := testutils.NewTestCluster()
	defer cluster.Cleanup()

	// Add first node
	node1, err := cluster.AddNode("node1")
	require.NoError(t, err, "Failed to add first node")
	node1.Config.Pulse.Mode = "active-passive" // Ensure active-passive mode
	err = node1.Start()
	require.NoError(t, err, "Failed to start first node")
	time.Sleep(500 * time.Millisecond)

	// Add second node
	node2, err := cluster.AddNode("node2")
	require.NoError(t, err, "Failed to add second node")
	node2.Config.Pulse.Mode = "active-passive" // Ensure active-passive mode
	err = node2.Start()
	require.NoError(t, err, "Failed to start second node")
	time.Sleep(500 * time.Millisecond)

	// Join nodes to form a cluster
	err = node2.Join(node1)
	require.NoError(t, err, "Failed to join second node to cluster")
	time.Sleep(1 * time.Second)

	// Stop nodes before restarting them
	if node1.Server != nil {
		node1.Server.Stop()
		time.Sleep(500 * time.Millisecond)
	}
	if node2.Server != nil {
		node2.Server.Stop()
		time.Sleep(500 * time.Millisecond)
	}

	// Restart node1 first
	err = node1.Start()
	require.NoError(t, err, "Failed to restart node1")
	time.Sleep(500 * time.Millisecond)

	// Restart node2 second
	err = node2.Start()
	require.NoError(t, err, "Failed to restart node2")
	time.Sleep(500 * time.Millisecond)

	// We need to manually promote node1 to active and demote node2 to passive
	// This is a workaround for the health checker automatically marking local nodes as active

	// First, let's check the current status
	t.Logf("Initial node1 status: %s", node1.GetMemberStatus(node1.Hostname))
	t.Logf("Initial node2 status: %s", node2.GetMemberStatus(node2.Hostname))

	// Set node statuses
	node1.SetStatus(testutils.StatusActive)
	node2.SetStatus(testutils.StatusPassive)

	// Give some time for the changes to take effect
	time.Sleep(2 * time.Second)

	// Log the updated statuses
	t.Logf("Updated node1 status: %s", node1.GetMemberStatus(node1.Hostname))
	t.Logf("Updated node2 status: %s", node2.GetMemberStatus(node2.Hostname))

	// Verify node statuses
	node1Status := node1.GetMemberStatus(node1.Hostname)
	node2Status := node2.GetMemberStatus(node2.Hostname)
	require.Equal(t, "active", node1Status, "Node1 should be active initially")
	require.Equal(t, "passive", node2Status, "Node2 should be passive initially")

	// Test Case 1: Enable quorum with majority mode (requires both nodes)
	t.Log("Test Case 1: Quorum with majority mode in active-passive setup")
	node1.Config.Pulse.QuorumEnabled = true
	node1.Config.Pulse.QuorumMajorityMode = true
	err = node1.Config.Save()
	require.NoError(t, err, "Failed to save config with quorum enabled")

	// Sync config to node2
	err = node1.SyncConfigWithNode(node2)
	require.NoError(t, err, "Failed to sync config to node2")
	time.Sleep(1 * time.Second)

	// Reload configs from disk
	err = node1.Config.Load()
	require.NoError(t, err, "Failed to reload node1 config")
	err = node2.Config.Load()
	require.NoError(t, err, "Failed to reload node2 config")

	// Verify quorum settings are properly synced
	require.True(t, node1.Config.Pulse.QuorumEnabled, "Quorum should be enabled on node1")
	require.True(t, node2.Config.Pulse.QuorumEnabled, "Quorum should be enabled on node2")
	require.True(t, node1.Config.Pulse.QuorumMajorityMode, "Quorum majority mode should be enabled on node1")
	require.True(t, node2.Config.Pulse.QuorumMajorityMode, "Quorum majority mode should be enabled on node2")

	// Test Case 2: Enable quorum with fixed mode (requires only 1 node)
	t.Log("Test Case 2: Quorum with fixed mode in active-passive setup")
	node1.Config.Pulse.QuorumMajorityMode = false
	node1.Config.Pulse.QuorumMinNodes = 1
	err = node1.Config.Save()
	require.NoError(t, err, "Failed to save config with fixed quorum mode")

	// Sync config to node2
	err = node1.SyncConfigWithNode(node2)
	require.NoError(t, err, "Failed to sync config to node2")
	time.Sleep(1 * time.Second)

	// Reload configs from disk
	err = node1.Config.Load()
	require.NoError(t, err, "Failed to reload node1 config")
	err = node2.Config.Load()
	require.NoError(t, err, "Failed to reload node2 config")

	// Verify quorum settings are properly synced
	require.True(t, node1.Config.Pulse.QuorumEnabled, "Quorum should be enabled on node1")
	require.True(t, node2.Config.Pulse.QuorumEnabled, "Quorum should be enabled on node2")
	require.False(t, node1.Config.Pulse.QuorumMajorityMode, "Quorum majority mode should be disabled on node1")
	require.False(t, node2.Config.Pulse.QuorumMajorityMode, "Quorum majority mode should be disabled on node2")
	require.Equal(t, 1, node1.Config.Pulse.QuorumMinNodes, "Quorum min nodes should be 1 on node1")
	require.Equal(t, 1, node2.Config.Pulse.QuorumMinNodes, "Quorum min nodes should be 1 on node2")
}
