package quorum

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/syleron/pulseha/rpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPCHandler implements the quorum-related RPC methods
type RPCHandler struct {
	rpc.UnimplementedServerServer
	rpc.UnimplementedCLIServer
	quorumManager *QuorumManager
	logger        *logrus.Logger
}

// NewRPCHandler creates a new RPC handler for quorum-related methods
func NewRPCHandler(quorumManager *QuorumManager, logger *logrus.Logger) *RPCHandler {
	return &RPCHandler{
		quorumManager: quorumManager,
		logger:        logger,
	}
}

// StartVotingSession handles RPC requests to start a new voting session
func (h *RPCHandler) StartVotingSession(ctx context.Context, req *rpc.StartVotingSessionRequest) (*rpc.StartVotingSessionResponse, error) {
	h.logger.Debugf("Received StartVotingSession request: %v", req)

	// Convert VoteType from RPC to internal type
	var voteType VoteType
	switch req.Type {
	case rpc.VoteType_NODE_STATUS:
		voteType = VoteTypeNodeStatus
	case rpc.VoteType_IP_REDISTRIBUTION:
		voteType = VoteTypeIPRedistribution
	case rpc.VoteType_CONFIG_CHANGE:
		voteType = VoteTypeConfigChange
	default:
		return &rpc.StartVotingSessionResponse{
			Success: false,
			Message: "Invalid vote type",
		}, status.Error(codes.InvalidArgument, "invalid vote type")
	}

	// Set a default timeout if not specified
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second // Default 30 second timeout
	}

	// Start the voting session
	sessionID, err := h.quorumManager.StartVotingSession(voteType, req.Subject, req.Description, timeout)
	if err != nil {
		return &rpc.StartVotingSessionResponse{
			Success: false,
			Message: err.Error(),
		}, status.Error(codes.Internal, err.Error())
	}

	return &rpc.StartVotingSessionResponse{
		Success:   true,
		Message:   "Voting session started successfully",
		SessionId: sessionID,
	}, nil
}

// CastVote handles RPC requests to cast a vote in a voting session
func (h *RPCHandler) CastVote(ctx context.Context, req *rpc.CastVoteRequest) (*rpc.CastVoteResponse, error) {
	h.logger.Debugf("Received CastVote request: %v", req)

	// Convert VoteDecision from RPC to internal type
	var decision VoteDecision
	switch req.Decision {
	case rpc.VoteDecision_YES:
		decision = VoteDecisionYes
	case rpc.VoteDecision_NO:
		decision = VoteDecisionNo
	case rpc.VoteDecision_ABSTAIN:
		decision = VoteDecisionAbstain
	default:
		return &rpc.CastVoteResponse{
			Success: false,
			Message: "Invalid vote decision",
		}, status.Error(codes.InvalidArgument, "invalid vote decision")
	}

	// Cast the vote
	err := h.quorumManager.CastVote(req.SessionId, req.VoterId, decision)
	if err != nil {
		return &rpc.CastVoteResponse{
			Success: false,
			Message: err.Error(),
		}, status.Error(codes.Internal, err.Error())
	}

	return &rpc.CastVoteResponse{
		Success: true,
		Message: "Vote cast successfully",
	}, nil
}

// GetVotingResult handles RPC requests to get the result of a voting session
func (h *RPCHandler) GetVotingResult(ctx context.Context, req *rpc.GetVotingResultRequest) (*rpc.GetVotingResultResponse, error) {
	h.logger.Debugf("Received GetVotingResult request: %v", req)

	// Get the voting session
	session, err := h.quorumManager.GetVotingSession(req.SessionId)
	if err != nil {
		return &rpc.GetVotingResultResponse{
			Success: false,
			Message: err.Error(),
		}, status.Error(codes.NotFound, err.Error())
	}

	// Check if the session has a result
	if session.Result == nil {
		return &rpc.GetVotingResultResponse{
			Success: false,
			Message: "Voting session has not concluded yet",
		}, nil
	}

	// Convert the result to RPC format
	result := &rpc.VotingSessionResult{
		Passed:      session.Result.Passed,
		YesCount:    int32(session.Result.YesCount),
		NoCount:     int32(session.Result.NoCount),
		TotalVotes:  int32(session.Result.TotalVotes),
		QuorumMet:   session.Result.QuorumMet,
		CompletedAt: session.Result.CompletedAt.Unix(),
	}

	return &rpc.GetVotingResultResponse{
		Success: true,
		Message: "Voting result retrieved successfully",
		Result:  result,
	}, nil
}

// GetVotingSessions handles RPC requests to list voting sessions
func (h *RPCHandler) GetVotingSessions(ctx context.Context, req *rpc.GetVotingSessionsRequest) (*rpc.GetVotingSessionsResponse, error) {
	h.logger.Debugf("Received GetVotingSessions request: %v", req)

	// Get active sessions
	activeSessions := h.quorumManager.GetActiveVotingSessions()

	// Convert sessions to RPC format
	rpcSessions := make([]*rpc.VotingSessionInfo, 0, len(activeSessions))
	for _, session := range activeSessions {
		// Convert VoteType to RPC format
		var voteType rpc.VoteType
		switch session.Type {
		case VoteTypeNodeStatus:
			voteType = rpc.VoteType_NODE_STATUS
		case VoteTypeIPRedistribution:
			voteType = rpc.VoteType_IP_REDISTRIBUTION
		case VoteTypeConfigChange:
			voteType = rpc.VoteType_CONFIG_CHANGE
		}

		// Create session info
		sessionInfo := &rpc.VotingSessionInfo{
			Id:          session.ID,
			Type:        voteType,
			Subject:     session.Subject,
			Description: session.Description,
			StartTime:   session.StartTime.Unix(),
			EndTime:     session.EndTime.Unix(),
			Completed:   session.Result != nil,
		}

		// Add result if available
		if session.Result != nil {
			sessionInfo.Result = &rpc.VotingSessionResult{
				Passed:      session.Result.Passed,
				YesCount:    int32(session.Result.YesCount),
				NoCount:     int32(session.Result.NoCount),
				TotalVotes:  int32(session.Result.TotalVotes),
				QuorumMet:   session.Result.QuorumMet,
				CompletedAt: session.Result.CompletedAt.Unix(),
			}
		}

		rpcSessions = append(rpcSessions, sessionInfo)
	}

	return &rpc.GetVotingSessionsResponse{
		Success:  true,
		Message:  "Voting sessions retrieved successfully",
		Sessions: rpcSessions,
	}, nil
}

// GetVotingSessionDetails handles RPC requests to get detailed information about a voting session
func (h *RPCHandler) GetVotingSessionDetails(ctx context.Context, req *rpc.GetVotingSessionDetailsRequest) (*rpc.GetVotingSessionDetailsResponse, error) {
	h.logger.Debugf("Received GetVotingSessionDetails request: %v", req)

	// Get the voting session
	session, err := h.quorumManager.GetVotingSession(req.SessionId)
	if err != nil {
		return &rpc.GetVotingSessionDetailsResponse{
			Success: false,
			Message: err.Error(),
		}, status.Error(codes.NotFound, err.Error())
	}

	// Convert VoteType to RPC format
	var voteType rpc.VoteType
	switch session.Type {
	case VoteTypeNodeStatus:
		voteType = rpc.VoteType_NODE_STATUS
	case VoteTypeIPRedistribution:
		voteType = rpc.VoteType_IP_REDISTRIBUTION
	case VoteTypeConfigChange:
		voteType = rpc.VoteType_CONFIG_CHANGE
	}

	// Create session info
	sessionInfo := &rpc.VotingSessionInfo{
		Id:          session.ID,
		Type:        voteType,
		Subject:     session.Subject,
		Description: session.Description,
		StartTime:   session.StartTime.Unix(),
		EndTime:     session.EndTime.Unix(),
		Completed:   session.Result != nil,
	}

	// Add result if available
	if session.Result != nil {
		sessionInfo.Result = &rpc.VotingSessionResult{
			Passed:      session.Result.Passed,
			YesCount:    int32(session.Result.YesCount),
			NoCount:     int32(session.Result.NoCount),
			TotalVotes:  int32(session.Result.TotalVotes),
			QuorumMet:   session.Result.QuorumMet,
			CompletedAt: session.Result.CompletedAt.Unix(),
		}
	}

	// Convert votes to RPC format
	votes := make([]*rpc.Vote, 0, len(session.Votes))
	for _, vote := range session.Votes {
		// Convert VoteDecision to RPC format
		var decision rpc.VoteDecision
		switch vote.Decision {
		case VoteDecisionYes:
			decision = rpc.VoteDecision_YES
		case VoteDecisionNo:
			decision = rpc.VoteDecision_NO
		case VoteDecisionAbstain:
			decision = rpc.VoteDecision_ABSTAIN
		}

		votes = append(votes, &rpc.Vote{
			VoterId:   vote.VoterID,
			Decision:  decision,
			Timestamp: vote.Timestamp.Unix(),
		})
	}

	return &rpc.GetVotingSessionDetailsResponse{
		Success: true,
		Message: "Voting session details retrieved successfully",
		Session: sessionInfo,
		Votes:   votes,
	}, nil
}
