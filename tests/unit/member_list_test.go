package unit

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/syleron/pulseha/internal/membership"
	"github.com/syleron/pulseha/packages/config"
)

func TestMemberList_AddMember(t *testing.T) {
	// Create config
	cfg := &config.Config{
		Pulse: config.Local{
			LocalNode: "node1",
		},
		Nodes: map[string]*config.Node{
			"node1": {
				Hostname: "node1",
				IP:       "127.0.0.1",
				Port:     "8080",
			},
		},
	}

	// Create logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create member list
	memberList := membership.NewMemberList(cfg, logger)

	// Test adding a member
	err := memberList.AddMember("node1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(memberList.Members))

	// Test adding duplicate member
	err = memberList.AddMember("node1")
	assert.Error(t, err)
	assert.Equal(t, 1, len(memberList.Members))

	// Test adding member with invalid config
	err = memberList.AddMember("invalid")
	assert.Error(t, err)
}

func TestMemberList_RemoveMember(t *testing.T) {
	// Create config
	cfg := &config.Config{
		Pulse: config.Local{
			LocalNode: "node1",
		},
		Nodes: map[string]*config.Node{
			"node1": {
				Hostname: "node1",
				IP:       "127.0.0.1",
				Port:     "8080",
			},
			"node2": {
				Hostname: "node2",
				IP:       "127.0.0.1",
				Port:     "8081",
			},
		},
	}

	// Create logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create member list
	memberList := membership.NewMemberList(cfg, logger)

	// Add members
	err := memberList.AddMember("node1")
	assert.NoError(t, err)
	err = memberList.AddMember("node2")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(memberList.Members))

	// Test removing a member
	err = memberList.RemoveMember("node2")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(memberList.Members))

	// Test removing non-existent member
	err = memberList.RemoveMember("invalid")
	assert.Error(t, err)
}

func TestMemberList_GetMemberByHostname(t *testing.T) {
	// Create config
	cfg := &config.Config{
		Pulse: config.Local{
			LocalNode: "node1",
		},
		Nodes: map[string]*config.Node{
			"node1": {
				Hostname: "node1",
				IP:       "127.0.0.1",
				Port:     "8080",
			},
		},
	}

	// Create logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create member list
	memberList := membership.NewMemberList(cfg, logger)

	// Add a member
	err := memberList.AddMember("node1")
	assert.NoError(t, err)

	// Test getting existing member
	member := memberList.GetMemberByHostname("node1")
	assert.NotNil(t, member)
	assert.Equal(t, "node1", member.Hostname)

	// Test getting non-existent member
	member = memberList.GetMemberByHostname("invalid")
	assert.Nil(t, member)
}

func TestMemberList_RedistributeIPs(t *testing.T) {
	// Create config
	cfg := &config.Config{
		Pulse: config.Local{
			LocalNode: "node1",
		},
		Nodes: map[string]*config.Node{
			"node1": {
				Hostname: "node1",
				IP:       "127.0.0.1",
				Port:     "8080",
			},
			"node2": {
				Hostname: "node2",
				IP:       "127.0.0.1",
				Port:     "8081",
			},
		},
	}

	// Create logger
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create member list
	memberList := membership.NewMemberList(cfg, logger)

	// Add members
	err := memberList.AddMember("node1")
	assert.NoError(t, err)
	err = memberList.AddMember("node2")
	assert.NoError(t, err)

	// Set node1 as active
	node1 := memberList.GetMemberByHostname("node1")
	assert.NotNil(t, node1)
	node1.Status = membership.StatusActive

	// Set node2 as active
	node2 := memberList.GetMemberByHostname("node2")
	assert.NotNil(t, node2)
	node2.Status = membership.StatusActive

	// Test IP redistribution
	failedIPs := []string{"192.168.1.1", "192.168.1.2"}
	err = memberList.RedistributeIPs(failedIPs)
	assert.NoError(t, err)

	// Verify IPs were distributed
	totalAssignedIPs := len(node1.ActiveIPs) + len(node2.ActiveIPs)
	assert.Equal(t, len(failedIPs), totalAssignedIPs)
}
