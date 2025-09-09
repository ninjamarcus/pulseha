package testutils

import (
	"time"

	log "github.com/charmbracelet/log"
	"github.com/syleron/pulseha/internal/quorum"
	"github.com/syleron/pulseha/packages/config"
)

// TestQuorumManager extends the quorum manager with test-specific functionality
type TestQuorumManager struct {
	quorumManager *quorum.QuorumManager
	nodeCount     int
	config        *config.Config
	logger        *log.Logger
}

// NewTestQuorumManager creates a new quorum manager for testing
func NewTestQuorumManager(cfg *config.Config, logger *log.Logger) *TestQuorumManager {
	return &TestQuorumManager{
		quorumManager: quorum.NewQuorumManager(cfg, logger),
		config:        cfg,
		logger:        logger,
		nodeCount:     len(cfg.Nodes),
	}
}

// UpdateNodeCount updates the node count in the quorum manager
func (q *TestQuorumManager) UpdateNodeCount(count int) {
	q.nodeCount = count
	q.quorumManager.UpdateNodeCount(count)
}

// StartTestVotingSession starts a new voting session for testing
func (q *TestQuorumManager) StartTestVotingSession(
	voteType quorum.VoteType,
	subject string,
	description string,
	timeout time.Duration,
) (string, error) {
	return q.quorumManager.StartVotingSession(voteType, subject, description, timeout)
}

// CastTestVote casts a vote in a voting session for testing
func (q *TestQuorumManager) CastTestVote(
	sessionID string,
	voterID string,
	decision quorum.VoteDecision,
) error {
	return q.quorumManager.CastVote(sessionID, voterID, decision)
}

// GetTestVotingSession gets a voting session for testing
func (q *TestQuorumManager) GetTestVotingSession(sessionID string) (*quorum.VotingSession, error) {
	return q.quorumManager.GetVotingSession(sessionID)
}

// ProcessTestExpiredSessions processes expired voting sessions for testing
func (q *TestQuorumManager) ProcessTestExpiredSessions() {
	q.quorumManager.ProcessExpiredSessions()
}

// CalculateQuorumMinimum calculates the minimum number of nodes required for quorum
func (q *TestQuorumManager) CalculateQuorumMinimum(cfg *config.Config, nodeCount int) int {
	if cfg.Pulse.QuorumMajorityMode {
		return (nodeCount / 2) + 1
	}
	return cfg.Pulse.QuorumMinNodes
}
