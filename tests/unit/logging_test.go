package unit

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/syleron/pulseha/packages/logging"
	"github.com/syleron/pulseha/rpc"
)

// TestLoggingFireMissingNode verifies that Fire handles entries without the
// "node" field gracefully and defaults to an empty string.
func TestLoggingFireMissingNode(t *testing.T) {
	var req *rpc.LogsRequest
	logger, _ := logging.NewLogger(func(r *rpc.LogsRequest) error {
		req = r
		return nil
	})

	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.InfoLevel,
		Message: "test message",
		Data:    logrus.Fields{},
	}

	assert.NotPanics(t, func() {
		err := logger.Fire(entry)
		assert.NoError(t, err)
	})
	assert.NotNil(t, req)
	assert.Equal(t, "", req.NodeId)
}
