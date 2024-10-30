package rpc

import (
	"context"
	"github.com/syleron/pulseha/rpc"
	"net"
	"sync"
)

type CLIServer struct {
	sync.Mutex
	Server   *Server
	Listener net.Listener
}

func (s *CLIServer) Join(ctx context.Context, in *rpc.JoinRequest) (*rpc.JoinResponse, error) {}

func (s *CLIServer) Leave(ctx context.Context, in *rpc.LeaveRequest) (*rpc.LeaveResponse, error) {}

func (s *CLIServer) Remove(ctx context.Context, in *rpc.RemoveRequest) (*rpc.RemoveResponse, error) {}

func (s *CLIServer) Create(ctx context.Context, in *rpc.CreateRequest) (*rpc.CreateResponse, error) {}

func (s *CLIServer) NewGroup(ctx context.Context, in *rpc.GroupNewRequest) (*rpc.GroupNewResponse, error) {
}

func (s *CLIServer) DeleteGroup(ctx context.Context, in *rpc.GroupDeleteRequest) (*rpc.GroupDeleteResponse, error) {
}

func (s *CLIServer) GroupIPAdd(ctx context.Context, in *rpc.GroupAddRequest) (*rpc.GroupAddResponse, error) {
}

func (s *CLIServer) GroupIPRemove(ctx context.Context, in *rpc.GroupRemoveRequest) (*rpc.GroupRemoveResponse, error) {
}

func (s *CLIServer) GroupAssign(ctx context.Context, in *rpc.GroupAssignRequest) (*rpc.GroupAssignResponse, error) {
}

func (s *CLIServer) GroupUnassign(ctx context.Context, in *rpc.GroupUnassignRequest) (*rpc.GroupUnassignResponse, error) {
}

func (s *CLIServer) GroupList(ctx context.Context, in *rpc.GroupTableRequest) (*rpc.GroupTableResponse, error) {
}

func (s *CLIServer) Status(ctx context.Context, in *rpc.StatusRequest) (*rpc.StatusResponse, error) {}

func (s *CLIServer) Promote(ctx context.Context, in *rpc.PromoteRequest) (*rpc.PromoteResponse, error) {
}

func (s *CLIServer) TLS(ctx context.Context, in *rpc.CertRequest) (*rpc.CertResponse, error) {}

func (s *CLIServer) Config(ctx context.Context, in *rpc.ConfigRequest) (*rpc.ConfigResponse, error) {}

func (s *CLIServer) Token(ctx context.Context, in *rpc.TokenRequest) (*rpc.TokenResponse, error) {}

func (s *CLIServer) Network(ctx context.Context, in *rpc.PulseNetwork) (*rpc.PulseNetwork, error) {}

func (s *CLIServer) Describe(ctx context.Context, in *rpc.DescribeRequest) (*rpc.DescribeResponse, error) {
}
