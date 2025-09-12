package unit

import (
	"testing"

	log "github.com/charmbracelet/log"
	"github.com/stretchr/testify/assert"
	"github.com/syleron/pulseha/internal/membership"
	"github.com/syleron/pulseha/packages/config"
)

func TestMemberList_AddMember(t *testing.T) {
	// Create a new config with a test node
	cfg := &config.Config{
		Nodes: map[string]*config.Node{
			"node1": {
				Hostname: "node1.example.com",
				IP:       "192.168.1.101",
				Port:     "8080",
			},
			"node2": {
				Hostname: "node2.example.com",
				IP:       "192.168.1.102",
				Port:     "8080",
			},
		},
	}

	// Create a new member list
	logger := log.New(nil)
	memberList := membership.NewMemberList(cfg, logger)

	// Test adding a new member
	err := memberList.AddMember("node1", "node1.example.com", "192.168.1.101", "8080")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(memberList.Members))

	// Test adding a duplicate member
	err = memberList.AddMember("node1", "node1.example.com", "192.168.1.101", "8080")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(memberList.Members))

	// Test adding another valid member present in config
	err = memberList.AddMember("node2", "node2.example.com", "192.168.1.102", "8080")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(memberList.Members))
}

func TestMemberList_GetMemberByID(t *testing.T) {
	// Create a new config with test nodes
	cfg := &config.Config{
		Nodes: map[string]*config.Node{
			"node1": {
				Hostname: "node1.example.com",
				IP:       "192.168.1.101",
				Port:     "8080",
			},
			"node2": {
				Hostname: "node2.example.com",
				IP:       "192.168.1.102",
				Port:     "8080",
			},
		},
	}

	// Create a new member list
	logger := log.New(nil)
	memberList := membership.NewMemberList(cfg, logger)

	// Add test members
	err := memberList.AddMember("node1", "node1.example.com", "192.168.1.101", "8080")
	assert.NoError(t, err)
	err = memberList.AddMember("node2", "node2.example.com", "192.168.1.102", "8080")
	assert.NoError(t, err)

	// Test getting existing member
	member := memberList.GetMemberByID("node1")
	assert.NotNil(t, member)
	assert.Equal(t, "node1", member.ID)

	// Test getting non-existent member
	member = memberList.GetMemberByID("node3")
	assert.Nil(t, member)
}

func TestMemberList_GetMemberByHostname(t *testing.T) {
	// Create a new config with test nodes
	cfg := &config.Config{
		Nodes: map[string]*config.Node{
			"node1": {
				Hostname: "node1.example.com",
				IP:       "192.168.1.101",
				Port:     "8080",
			},
		},
	}

	// Create a new member list
	logger := log.New(nil)
	memberList := membership.NewMemberList(cfg, logger)

	// Add test member
	err := memberList.AddMember("node1", "node1.example.com", "192.168.1.101", "8080")
	assert.NoError(t, err)

	// Test getting existing member
	member := memberList.GetMemberByHostname("node1.example.com")
	assert.NotNil(t, member)
	assert.Equal(t, "node1.example.com", member.Hostname)

	// Test getting non-existent member
	member = memberList.GetMemberByHostname("node2.example.com")
	assert.Nil(t, member)
}

func TestMemberList_RemoveMember(t *testing.T) {
	// Create a new config with test nodes
	cfg := &config.Config{
		Nodes: map[string]*config.Node{
			"node1": {
				Hostname: "node1.example.com",
				IP:       "192.168.1.101",
				Port:     "8080",
			},
			"node2": {
				Hostname: "node2.example.com",
				IP:       "192.168.1.102",
				Port:     "8080",
			},
		},
	}

	// Create a new member list
	logger := log.New(nil)
	memberList := membership.NewMemberList(cfg, logger)

	// Add test members
	err := memberList.AddMember("node1", "node1.example.com", "192.168.1.101", "8080")
	assert.NoError(t, err)
	err = memberList.AddMember("node2", "node2.example.com", "192.168.1.102", "8080")
	assert.NoError(t, err)

	// Test removing existing member
	err = memberList.RemoveMember("node1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(memberList.Members))

	// Test removing non-existent member
	err = memberList.RemoveMember("node3")
	assert.Error(t, err)
	assert.Equal(t, 1, len(memberList.Members))
}

func TestMemberList_RedistributeIPs(t *testing.T) {
	// Redistribution now relies on server-driven orchestration and network helpers.
	// This scenario is covered by integration tests.
	t.Skip("RedistributeIPs behavior depends on server IP orchestration; covered in integration tests")
}
