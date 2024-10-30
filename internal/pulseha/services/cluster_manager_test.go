package services

import (
	"github.com/syleron/pulseha/internal/pulseha/models"
	"reflect"
	"sync"
	"testing"
)

func TestClusterManager(t *testing.T) {
	// Create a new cluster manager
	clusterManager := NewClusterManager()

	// Define 4 test nodes
	nodes := []*models.Node{
		{Hostname: "node1", Status: nil, FailoverScore: 10},
		{Hostname: "node2", Status: nil, FailoverScore: 20},
		{Hostname: "node3", Status: nil, FailoverScore: 30},
		{Hostname: "node4", Status: nil, FailoverScore: 40},
	}

	// Add nodes to the cluster
	for _, node := range nodes {
		clusterManager.AddNode(node)
	}

	// Test if the nodes were added correctly
	if len(clusterManager.nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(clusterManager.nodes))
	}

	// Remove a node and check
	clusterManager.RemoveNode("node3")
	if len(clusterManager.nodes) != 3 {
		t.Fatalf("expected 3 nodes after removal, got %d", len(clusterManager.nodes))
	}

	//// Test promoting a node
	//clusterManager.MakeNodeActive()
	//// Validate the state change
	//if clusterManager.nodes["node1"].Status != models.Active {
	//	t.Fatalf("expected node1 to be active")
	//}
	//
	//// Test demoting a node
	//clusterManager.MakeNodePassive()
	//// Validate the state change
	//if clusterManager.nodes["node1"].Status != models.Passive {
	//	t.Fatalf("expected node1 to be passive")
	//}
}

func TestClusterManager_AddNode(t *testing.T) {
	type fields struct {
		nodes map[string]*models.Node
		Mutex sync.Mutex
	}
	type args struct {
		node *models.Node
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int // Expected number of nodes after adding
	}{
		{
			name: "Add single node",
			fields: fields{
				nodes: make(map[string]*models.Node),
			},
			args: args{
				node: &models.Node{Hostname: "node1", Status: nil},
			},
			want: 1,
		},
		{
			name: "Add multiple nodes",
			fields: fields{
				nodes: map[string]*models.Node{
					"node1": {Hostname: "node1", Status: nil},
				},
			},
			args: args{
				node: &models.Node{Hostname: "node2", Status: nil},
			},
			want: 2,
		},
		{
			name: "Add duplicate node",
			fields: fields{
				nodes: map[string]*models.Node{
					"node1": {Hostname: "node1", Status: nil},
				},
			},
			args: args{
				node: &models.Node{Hostname: "node1", Status: nil},
			},
			want: 1, // Node count should remain 1 since it's a duplicate
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClusterManager{
				nodes: tt.fields.nodes,
				Mutex: tt.fields.Mutex,
			}
			c.AddNode(tt.args.node)
			if len(c.nodes) != tt.want {
				t.Errorf("AddNode() = %v, want %v", len(c.nodes), tt.want)
			}
		})
	}
}

func TestClusterManager_JoinCluster(t *testing.T) {
	type fields struct {
		nodes map[string]*models.Node
		Mutex sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClusterManager{
				nodes: tt.fields.nodes,
				Mutex: tt.fields.Mutex,
			}
			c.JoinCluster()
		})
	}
}

func TestClusterManager_LeaveCluster(t *testing.T) {
	type fields struct {
		nodes map[string]*models.Node
		Mutex sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClusterManager{
				nodes: tt.fields.nodes,
				Mutex: tt.fields.Mutex,
			}
			c.LeaveCluster()
		})
	}
}

func TestClusterManager_MakeNodeActive(t *testing.T) {
	type fields struct {
		nodes map[string]*models.Node
		Mutex sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClusterManager{
				nodes: tt.fields.nodes,
				Mutex: tt.fields.Mutex,
			}
			c.MakeNodeActive()
		})
	}
}

func TestClusterManager_MakeNodePassive(t *testing.T) {
	type fields struct {
		nodes map[string]*models.Node
		Mutex sync.Mutex
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClusterManager{
				nodes: tt.fields.nodes,
				Mutex: tt.fields.Mutex,
			}
			c.MakeNodePassive()
		})
	}
}

func TestClusterManager_RemoveNode(t *testing.T) {
	type fields struct {
		nodes map[string]*models.Node
		Mutex sync.Mutex
	}
	type args struct {
		nodeID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int // Expected number of nodes after removal
	}{
		{
			name: "Remove existing node",
			fields: fields{
				nodes: map[string]*models.Node{
					"node1": {Hostname: "node1"},
					"node2": {Hostname: "node2"},
				},
			},
			args: args{
				nodeID: "node1",
			},
			want: 1, // After removing node1, 1 node should remain
		},
		{
			name: "Remove non-existing node",
			fields: fields{
				nodes: map[string]*models.Node{
					"node1": {Hostname: "node1"},
					"node2": {Hostname: "node2"},
				},
			},
			args: args{
				nodeID: "node3", // Node3 doesn't exist
			},
			want: 2, // No changes, still 2 nodes
		},
		{
			name: "Remove last node",
			fields: fields{
				nodes: map[string]*models.Node{
					"node1": {Hostname: "node1"},
				},
			},
			args: args{
				nodeID: "node1",
			},
			want: 0, // After removing node1, no nodes should remain
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClusterManager{
				nodes: tt.fields.nodes,
				Mutex: tt.fields.Mutex,
			}
			c.RemoveNode(tt.args.nodeID)

			// Check the remaining number of nodes
			if len(c.nodes) != tt.want {
				t.Errorf("RemoveNode() = %v, want %v", len(c.nodes), tt.want)
			}
		})
	}
}

func TestNewClusterManager(t *testing.T) {
	tests := []struct {
		name string
		want *ClusterManager
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewClusterManager(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewClusterManager() = %v, want %v", got, tt.want)
			}
		})
	}
}
