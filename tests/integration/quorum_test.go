package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuorumMajorityCalculation(t *testing.T) {
	// Table-driven verification of majority formula
	testCases := []struct {
		nodeCount int
		expected  int // Expected minimum votes for quorum
	}{
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{5, 3},
		{6, 4},
	}

	for _, tc := range testCases {
		minVotes := (tc.nodeCount / 2) + 1
		require.Equal(t, tc.expected, minVotes,
			"For %d nodes, expected minimum votes for quorum to be %d, got %d",
			tc.nodeCount, tc.expected, minVotes)
	}
}
