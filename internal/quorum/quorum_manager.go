package quorum

import (
	"fmt"
	"sync"
	"time"

	log "github.com/charmbracelet/log"
	"github.com/google/uuid"
	"github.com/syleron/pulseha/packages/config"
)

// VoteType represents the type of vote being cast
type VoteType string

const (
	// VoteTypeNodeStatus is used for voting on node status changes
	VoteTypeNodeStatus VoteType = "node_status"
	// VoteTypeIPRedistribution is used for voting on IP redistribution
	VoteTypeIPRedistribution VoteType = "ip_redistribution"
	// VoteTypeConfigChange is used for voting on configuration changes
	VoteTypeConfigChange VoteType = "config_change"
)

// VoteDecision represents a vote decision
type VoteDecision string

const (
	// VoteDecisionYes represents a yes vote
	VoteDecisionYes VoteDecision = "yes"
	// VoteDecisionNo represents a no vote
	VoteDecisionNo VoteDecision = "no"
	// VoteDecisionAbstain represents an abstention
	VoteDecisionAbstain VoteDecision = "abstain"
)

// Vote represents a single vote cast by a node
type Vote struct {
	VoterID   string       // ID of the node casting the vote
	Decision  VoteDecision // The vote decision
	Timestamp time.Time    // When the vote was cast
}

// VotingSession represents an active or completed voting session
type VotingSession struct {
	ID          string               // Unique ID for this voting session
	Type        VoteType             // Type of vote
	Subject     string               // What is being voted on (node ID, IP, etc.)
	Description string               // Human-readable description
	StartTime   time.Time            // When the voting started
	EndTime     time.Time            // When the voting will end
	Votes       map[string]Vote      // Map of node ID to vote
	Result      *VotingSessionResult // Result of the vote, nil if not completed
}

// VotingSessionResult represents the result of a completed voting session
type VotingSessionResult struct {
	Passed      bool      // Whether the vote passed
	YesCount    int       // Number of yes votes
	NoCount     int       // Number of no votes
	TotalVotes  int       // Total number of votes cast
	QuorumMet   bool      // Whether quorum was met
	CompletedAt time.Time // When the voting completed
}

// QuorumManager handles quorum-based voting for cluster decisions
type QuorumManager struct {
	sync.RWMutex
	config         *config.Config
	logger         *log.Logger
	activeSessions map[string]*VotingSession
	sessionHistory map[string]*VotingSession
	nodeCount      int // Total number of nodes in the cluster
}

// NewQuorumManager creates a new quorum manager instance
func NewQuorumManager(cfg *config.Config, logger *log.Logger) *QuorumManager {
	return &QuorumManager{
		config:         cfg,
		logger:         logger,
		activeSessions: make(map[string]*VotingSession),
		sessionHistory: make(map[string]*VotingSession),
		nodeCount:      len(cfg.Nodes),
	}
}

// UpdateNodeCount updates the total number of nodes in the cluster
// and automatically adjusts quorum settings based on node count
func (q *QuorumManager) UpdateNodeCount(count int) {
	q.Lock()
	defer q.Unlock()

	q.nodeCount = count
	// Quorum behavior is automatic now; no config toggles to persist
}

// StartVotingSession creates a new voting session and returns its ID
func (q *QuorumManager) StartVotingSession(voteType VoteType, subject string, description string, timeout time.Duration) (string, error) {
	q.Lock()
	defer q.Unlock()

	// Require at least 3 nodes to start a voting session
	if q.nodeCount < 3 {
		return "", fmt.Errorf("quorum voting requires at least 3 nodes")
	}

	// Generate a unique session ID
	sessionID := uuid.New().String()

	// Create the voting session
	session := &VotingSession{
		ID:          sessionID,
		Type:        voteType,
		Subject:     subject,
		Description: description,
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(timeout),
		Votes:       make(map[string]Vote),
	}

	// Add to active sessions
	q.activeSessions[sessionID] = session

	q.logger.Infof("Started voting session %s for %s: %s", sessionID, voteType, description)
	return sessionID, nil
}

// CastVote records a vote for a specific voting session
func (q *QuorumManager) CastVote(sessionID string, voterID string, decision VoteDecision) error {
	q.Lock()
	defer q.Unlock()

	// Find the voting session
	session, exists := q.activeSessions[sessionID]
	if !exists {
		// Check if it's in the history
		session, exists = q.sessionHistory[sessionID]
		if !exists {
			return fmt.Errorf("voting session %s not found", sessionID)
		}
		return fmt.Errorf("voting session %s has already concluded", sessionID)
	}

	// Record the vote
	session.Votes[voterID] = Vote{
		VoterID:   voterID,
		Decision:  decision,
		Timestamp: time.Now(),
	}

	q.logger.Debugf("Recorded vote from %s for session %s: %s", voterID, sessionID, decision)

	// Check if we can conclude the voting
	if q.canConcludeVoting(session) {
		q.concludeVotingSessionLocked(sessionID)
	}

	return nil
}

// GetVotingSession returns information about a specific voting session
func (q *QuorumManager) GetVotingSession(sessionID string) (*VotingSession, error) {
	q.RLock()
	defer q.RUnlock()

	// Check active sessions
	session, exists := q.activeSessions[sessionID]
	if exists {
		return session, nil
	}

	// Check session history
	session, exists = q.sessionHistory[sessionID]
	if exists {
		return session, nil
	}

	return nil, fmt.Errorf("voting session %s not found", sessionID)
}

// GetActiveVotingSessions returns a list of all active voting sessions
func (q *QuorumManager) GetActiveVotingSessions() []*VotingSession {
	q.RLock()
	defer q.RUnlock()

	sessions := make([]*VotingSession, 0, len(q.activeSessions))
	for _, session := range q.activeSessions {
		sessions = append(sessions, session)
	}

	return sessions
}

// ProcessExpiredSessions checks for and concludes any expired voting sessions
func (q *QuorumManager) ProcessExpiredSessions() {
	q.Lock()
	defer q.Unlock()

	now := time.Now()
	for sessionID, session := range q.activeSessions {
		if now.After(session.EndTime) {
			q.logger.Infof("Voting session %s has expired, concluding", sessionID)
			q.concludeVotingSessionLocked(sessionID)
		}
	}
}

// HasQuorum determines if the given vote count meets quorum requirements
func (q *QuorumManager) HasQuorum(voteCount int) bool {
	q.RLock()
	defer q.RUnlock()

	// With fewer than 3 nodes, quorum logic is not applicable
	if q.nodeCount < 3 {
		return true
	}

	// Majority of current node count
	minVotes := (q.nodeCount / 2) + 1
	return voteCount >= minVotes
}

// canConcludeVoting checks if a voting session can be concluded early
// This happens if:
// 1. All nodes have voted, or
// 2. Enough YES votes to pass, or
// 3. Enough NO votes to fail
func (q *QuorumManager) canConcludeVoting(session *VotingSession) bool {
	// If all nodes have voted, we can conclude
	if len(session.Votes) >= q.nodeCount {
		return true
	}

	// Count votes
	yesCount := 0
	noCount := 0
	for _, vote := range session.Votes {
		switch vote.Decision {
		case VoteDecisionYes:
			yesCount++
		case VoteDecisionNo:
			noCount++
		}
	}

	// If we have enough YES votes to guarantee passage
	if q.HasQuorum(yesCount) {
		return true
	}

	// If we have enough NO votes to guarantee failure
	remainingPossibleYes := q.nodeCount - len(session.Votes)
	minVotes := (q.nodeCount / 2) + 1
	if yesCount+remainingPossibleYes < minVotes {
		return true
	}

	return false
}

// concludeVotingSession concludes a voting session and computes the result
func (q *QuorumManager) concludeVotingSession(sessionID string) {
	q.Lock()
	defer q.Unlock()
	q.concludeVotingSessionLocked(sessionID)
}

// concludeVotingSessionLocked is the internal implementation of concludeVotingSession
// that assumes the lock is already held
func (q *QuorumManager) concludeVotingSessionLocked(sessionID string) {
	// Find the session
	session, exists := q.activeSessions[sessionID]
	if !exists {
		q.logger.Warnf("Attempted to conclude non-existent voting session %s", sessionID)
		return
	}

	// Count votes
	yesCount := 0
	noCount := 0
	abstainCount := 0

	for _, vote := range session.Votes {
		switch vote.Decision {
		case VoteDecisionYes:
			yesCount++
		case VoteDecisionNo:
			noCount++
		case VoteDecisionAbstain:
			abstainCount++
		}
	}

	totalVotes := len(session.Votes)
	quorumMet := q.HasQuorum(totalVotes)

	// Determine if the vote passed
	// A vote passes if:
	// 1. Quorum was met, and
	// 2. More YES votes than NO votes
	passed := quorumMet && yesCount > noCount

	// Create the result
	session.Result = &VotingSessionResult{
		Passed:      passed,
		YesCount:    yesCount,
		NoCount:     noCount,
		TotalVotes:  totalVotes,
		QuorumMet:   quorumMet,
		CompletedAt: time.Now(),
	}

	// Move from active to history
	delete(q.activeSessions, sessionID)
	q.sessionHistory[sessionID] = session

	q.logger.Infof("Concluded voting session %s: passed=%v, quorum=%v, yes=%d, no=%d, total=%d",
		sessionID, passed, quorumMet, yesCount, noCount, totalVotes)
}

// Start starts the quorum manager
func (q *QuorumManager) Start() {
	go q.sessionExpiryLoop()
}

// Stop stops the quorum manager
func (q *QuorumManager) Stop() {
	// Nothing to do for now
}

// sessionExpiryLoop periodically checks for and concludes expired voting sessions
func (q *QuorumManager) sessionExpiryLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		q.ProcessExpiredSessions()
	}
}
