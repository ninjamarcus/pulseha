package unit

import (
	contextpkg "context"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/syleron/pulseha/internal/membership"
	"github.com/syleron/pulseha/internal/server"
	"github.com/syleron/pulseha/packages/config"
	"github.com/syleron/pulseha/packages/security"
	rpc "github.com/syleron/pulseha/rpc"
)

// TestCreateClusterReturnsWithoutDeadlock ensures CreateCluster no longer blocks
// (previously could deadlock by calling Reconfigure while holding the server lock).
func TestCreateClusterReturnsWithoutDeadlock(t *testing.T) {
	// Use test mode to avoid writing config files
	_ = os.Setenv("PULSEHA_TEST", "true")

	// Redirect cert directory to a temp dir to avoid writing into HOME
	tmpDir := t.TempDir()
	security.CertDir = tmpDir

	// Create config and logger
	cfg := config.New()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	// Create member list and health checker
	ml := membership.NewMemberList(cfg, logger)
	hc := membership.NewHealthChecker(ml, logger)

	// Create server instance (do not call Start to avoid binding default ports)
	s := server.NewServer(cfg, logger, ml, hc)

	// Prepare request using ephemeral port "0" to avoid port conflicts
	req := &rpc.CreateClusterRequest{
		BindIp:   "127.0.0.1",
		BindPort: "0",
		Mode:     "active-passive",
	}

	// Call CreateCluster in a goroutine and enforce timeout
	done := make(chan struct{})
	var resp *rpc.CreateClusterResponse
	var err error
	go func() {
		defer close(done)
		resp, err = s.CreateCluster(contextpkg.Background(), req)
	}()

	select {
	case <-done:
		// proceed
	case <-time.After(5 * time.Second):
		t.Fatal("CreateCluster did not return within timeout (possible deadlock)")
	}

	if err != nil {
		t.Fatalf("CreateCluster returned error: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Fatalf("CreateCluster unsuccessful response: %+v", resp)
	}
}
