package integration

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/syleron/pulseha/internal/quorum"
	"github.com/syleron/pulseha/tests/testutils"
)

// TestQuorumAutoManagement tests the automatic management of quorum settings
// based on the number of nodes in the cluster
func TestQuorumAutoManagement(t *testing.T) {
	// Create a new test cluster
	cluster := testutils.NewTestCluster()
	defer cluster.Cleanup()

	// Add and start first node
	node1, err := cluster.AddNode("node1")
	require.NoError(t, err, "Failed to add first node")
	err = node1.Start()
	require.NoError(t, err, "Failed to start first node")

	// Wait for node to be ready
	time.Sleep(500 * time.Millisecond)

	// Verify quorum is disabled with only one node
	require.False(t, node1.Config.Pulse.QuorumEnabled,
		"Quorum should be disabled with only one node")

	// Add and start second node
	node2, err := cluster.AddNode("node2")
	require.NoError(t, err, "Failed to add second node")
	err = node2.Start()
	require.NoError(t, err, "Failed to start second node")

	// Join second node to first node
	err = node2.Join(node1)
	require.NoError(t, err, "Failed to join second node to cluster")

	// Wait for cluster to stabilize
	time.Sleep(1 * time.Second)

	// Verify quorum is still disabled with two nodes
	require.False(t, node1.Config.Pulse.QuorumEnabled,
		"Quorum should be disabled with only two nodes")
	require.False(t, node2.Config.Pulse.QuorumEnabled,
		"Quorum should be disabled with only two nodes")

	// Add and start third node
	node3, err := cluster.AddNode("node3")
	require.NoError(t, err, "Failed to add third node")
	err = node3.Start()
	require.NoError(t, err, "Failed to start third node")

	// Join third node to first node
	err = node3.Join(node1)
	require.NoError(t, err, "Failed to join third node to cluster")

	// Wait for cluster to stabilize
	time.Sleep(1 * time.Second)

	// Verify quorum is automatically enabled with three nodes
	require.True(t, node1.Config.Pulse.QuorumEnabled,
		"Quorum should be enabled with three nodes")
	require.True(t, node2.Config.Pulse.QuorumEnabled,
		"Quorum should be enabled with three nodes")
	require.True(t, node3.Config.Pulse.QuorumEnabled,
		"Quorum should be enabled with three nodes")

	// Verify quorum minimum is set to majority (2 out of 3)
	require.Equal(t, 2, node1.Config.Pulse.QuorumMinNodes,
		"Quorum minimum should be 2 with three nodes")
	require.True(t, node1.Config.Pulse.QuorumMajorityMode,
		"Quorum majority mode should be enabled")

	// Test quorum adjustment when a node leaves
	err = node3.Leave()
	require.NoError(t, err, "Failed to leave cluster")

	// Wait for cluster to stabilize
	time.Sleep(1 * time.Second)

	// Verify quorum is automatically disabled with two nodes
	require.False(t, node1.Config.Pulse.QuorumEnabled,
		"Quorum should be disabled after node leaves (2 nodes)")
	require.False(t, node2.Config.Pulse.QuorumEnabled,
		"Quorum should be disabled after node leaves (2 nodes)")
}

// TestQuorumVotingWithAutoManagement tests that voting works correctly
// with the automatic quorum management
func TestQuorumVotingWithAutoManagement(t *testing.T) {
	// Create a new test cluster with 3 nodes
	cluster := testutils.NewTestCluster()
	defer cluster.Cleanup()

	// Add and start three nodes
	node1, err := cluster.AddNode("node1")
	require.NoError(t, err, "Failed to add first node")
	err = node1.Start()
	require.NoError(t, err, "Failed to start first node")

	node2, err := cluster.AddNode("node2")
	require.NoError(t, err, "Failed to add second node")
	err = node2.Start()
	require.NoError(t, err, "Failed to start second node")

	node3, err := cluster.AddNode("node3")
	require.NoError(t, err, "Failed to add third node")
	err = node3.Start()
	require.NoError(t, err, "Failed to start third node")

	// Join nodes to form a cluster
	err = node2.Join(node1)
	require.NoError(t, err, "Failed to join second node to cluster")
	err = node3.Join(node1)
	require.NoError(t, err, "Failed to join third node to cluster")

	// Wait for cluster to stabilize
	time.Sleep(1 * time.Second)

	// Verify quorum is enabled
	require.True(t, node1.Config.Pulse.QuorumEnabled,
		"Quorum should be enabled with three nodes")

	// Create a test quorum manager for node1
	logger := logrus.New()
	testQuorum := testutils.NewTestQuorumManager(node1.Config, logger)

	// Start a voting session
	sessionID, err := testQuorum.StartTestVotingSession(
		quorum.VoteTypeNodeStatus,
		"test-subject",
		"Test voting session",
		10*time.Second,
	)
	require.NoError(t, err, "Failed to start voting session")

	// Cast votes from all nodes
	err = testQuorum.CastTestVote(sessionID, node1.ID, quorum.VoteDecisionYes)
	require.NoError(t, err, "Failed to cast vote from node1")
	err = testQuorum.CastTestVote(sessionID, node2.ID, quorum.VoteDecisionYes)
	require.NoError(t, err, "Failed to cast vote from node2")

	// Get the voting session
	session, err := testQuorum.GetTestVotingSession(sessionID)
	require.NoError(t, err, "Failed to get voting session")

	// Verify that with 2 out of 3 votes, the vote passes (majority)
	testQuorum.ProcessTestExpiredSessions()
	session, err = testQuorum.GetTestVotingSession(sessionID)
	require.NoError(t, err, "Failed to get voting session")
	require.NotNil(t, session.Result, "Voting result should not be nil")
	require.True(t, session.Result.Passed, "Vote should pass with 2 out of 3 votes")
	require.True(t, session.Result.QuorumMet, "Quorum should be met with 2 out of 3 votes")

	// Now simulate a node leaving and test with 2 nodes
	err = node3.Leave()
	require.NoError(t, err, "Failed to leave cluster")

	// Wait for cluster to stabilize
	time.Sleep(1 * time.Second)

	// Verify quorum is disabled
	require.False(t, node1.Config.Pulse.QuorumEnabled,
		"Quorum should be disabled with two nodes")

	// Start another voting session
	sessionID2, err := testQuorum.StartTestVotingSession(
		quorum.VoteTypeNodeStatus,
		"test-subject-2",
		"Test voting session 2",
		10*time.Second,
	)
	require.NoError(t, err, "Failed to start second voting session")

	// Cast only one vote
	err = testQuorum.CastTestVote(sessionID2, node1.ID, quorum.VoteDecisionYes)
	require.NoError(t, err, "Failed to cast vote from node1")

	// Process the session
	testQuorum.ProcessTestExpiredSessions()
	session2, err := testQuorum.GetTestVotingSession(sessionID2)
	require.NoError(t, err, "Failed to get second voting session")

	// Verify that with quorum disabled, even one vote passes
	require.NotNil(t, session2.Result, "Voting result should not be nil")
	require.True(t, session2.Result.Passed, "Vote should pass with quorum disabled")
	require.True(t, session2.Result.QuorumMet, "Quorum should be considered met with quorum disabled")
}
