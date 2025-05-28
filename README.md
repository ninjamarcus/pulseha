   ___       __        __ _____ 
  / _ \__ __/ /__ ___ / // / _ |
 / ___/ // / (_-</ -_) _  / __ |
/_/   \_,_/_/___/\__/_//_/_/ |_|

```

PulseHA is a powerful high availability cluster management system designed to provide automated failover and IP address management across multiple nodes in a cluster.

## Why
PulseHA attempts to solve high availability with a more simple approach but without restricting functionality with the use of additional custom plugins.

## Features

- **Multiple Operating Modes**
  - Active-Passive: Traditional HA setup with one active node and multiple passive backups
  - Active-Active: Distributed load across multiple active nodes with intelligent IP distribution

- **Automatic Failover**
  - Health monitoring of cluster nodes
  - Automatic IP redistribution on node failure
  - Configurable failover thresholds and intervals
  - Auto-failback capability when failed nodes recover

- **IP Management**
  - Floating IP group management
  - Dynamic IP distribution based on node capacity
  - Graceful IP failover and failback
  - GARP (Gratuitous ARP) support for network updates

- **Security**
  - TLS encryption for inter-node communication
  - Certificate-based authentication
  - Secure cluster join tokens

- **Monitoring & Status**
  - Real-time cluster health monitoring
  - Node status tracking
  - Latency measurements
  - Detailed logging with configurable levels

## Installation

```bash
# Clone the repository
git clone https://github.com/syleron/pulseha.git

# Build the project
cd pulseha
go build -o pulseha cmd/pulseha/main.go
```

## Configuration

The configuration file is stored in `~/.pulseha/config.json` by default. Key configuration options include:

```json
{
  "pulseha": {
    "hcs_interval": 1000,
    "fos_interval": 5000,
    "fo_limit": 10000,
    "local_node": "",
    "cluster_token": "",
    "logging_level": "info",
    "auto_failback": true,
    "log_to_file": true,
    "log_file_location": "~/.pulseha/pulseha.log",
    "mode": "active-passive"
  },
  "floating_ip_groups": {},
  "nodes": {},
  "plugins": {}
}
```

## Usage

### Creating a Cluster

```bash
# Create a new cluster in active-passive mode (default)
pulseha cluster create --bind-ip <IP>

# Create a new cluster in active-active mode
pulseha cluster create --bind-ip <IP> --active-active
```

### Joining a Cluster

```bash
# Join an existing cluster
pulseha cluster join --hostname <EXISTING_NODE> --token <TOKEN>
```

### Managing Cluster Mode

```bash
# Set cluster mode
pulseha cluster mode set --mode active-passive
pulseha cluster mode set --mode active-active
```

### Managing IP Groups

```bash
# Create a new IP group
pulseha group create --name <GROUP_NAME>

# List all IP groups and their assignments
pulseha group list

# List groups in JSON format
pulseha group list --json

# Add IPs to a group
pulseha group add-ip --group <GROUP_NAME> --ip <IP_ADDRESS>

# Assign group to interface
pulseha group assign --group <GROUP_NAME> --hostname <NODE> --interface <IFACE>
```

### Monitoring

```bash
# View cluster status
pulseha status

# View status in JSON format
pulseha status --json
```

### Node Management

```bash
# Promote a node to active state
pulseha node promote --hostname <NODE>

# Remove a node from the cluster
pulseha node remove --hostname <NODE>
```

## Architecture

PulseHA uses a distributed architecture where each node maintains its own state and communicates with other nodes via gRPC. Key components include:

- **Member Management**: Handles node status, health checks, and cluster membership
- **IP Distribution**: Manages floating IP assignment based on cluster mode
- **Health Checker**: Monitors node health and triggers failover when needed
- **Configuration Sync**: Ensures cluster-wide configuration consistency

## Failover Logic

PulseHA implements a sophisticated failover mechanism to ensure high availability of services. The failover logic differs based on the cluster mode (active-passive or active-active).

### Health Checking Process

1. **Regular Health Checks**: Each node performs health checks on all other nodes in the cluster at configurable intervals (default: 1 second).
   
2. **Connection Verification**: Health checks include basic TCP connectivity tests to verify that nodes are reachable.
   
3. **Latency Measurement**: Response times are measured and recorded to help identify performance degradation.
   
4. **IP Verification**: For nodes hosting floating IPs, additional checks verify that the assigned IPs are properly functioning.

### Failure Detection

1. **Failure Thresholds**: A node is considered failed after multiple consecutive failed health checks (configurable via `fo_limit`).
   
2. **Partial Failures**: PulseHA can detect partial failures where only specific IPs on a node have failed while the node itself remains operational.
   
3. **Status Transitions**: Nodes transition through different states (Active, Passive, Unknown, PartialActive) based on health check results.

### Failover Process in Active-Passive Mode

1. **Active Node Failure**: When the active node fails:
   - The node's status changes to `Unknown`
   - All floating IPs are redistributed to a passive node
   - One passive node is promoted to active status
   
2. **IP Reassignment**: The newly promoted active node:
   - Brings up all floating IPs on its configured interfaces
   - Sends Gratuitous ARP packets to update network switches
   - Takes over all services associated with those IPs

3. **Recovery**: When the failed node recovers:
   - It rejoins the cluster as a passive node
   - If auto-failback is enabled, it may be promoted back to active after a stabilization period

### Failover Process in Active-Active Mode

1. **Node Failure**: When a node in active-active mode fails:
   - The node's status changes to `Unknown`
   - Only the IPs hosted by the failed node are redistributed
   - Other nodes remain in their current state
   
2. **IP Distribution**: Failed IPs are distributed among remaining nodes based on:
   - Current load factor of each node
   - Configured capacity of each node
   - Network topology considerations
   
3. **Partial Activation**: Nodes receiving redistributed IPs may become `PartialActive` if they weren't already active.

4. **Recovery**: When a failed node recovers:
   - It rejoins as a passive node
   - If auto-failback is enabled, it gradually receives a portion of IPs back
   - Load is rebalanced across all available nodes

### Configuration Synchronization

1. **Automatic Sync**: Configuration changes (like creating groups or adding IPs) are automatically synchronized to all nodes.

2. **Consistency Checks**: Regular verification ensures all nodes have consistent configuration.

3. **Conflict Resolution**: In case of conflicts, a deterministic resolution strategy is applied based on node priority.

### Handling Network Partitions

1. **Split-Brain Prevention**: PulseHA implements mechanisms to prevent split-brain scenarios where multiple nodes believe they are active.

2. **Quorum-Based Decisions**: In clusters with more than two nodes, quorum is used to determine which partition should remain active.

3. **Fencing**: In severe cases, automatic fencing can be configured to ensure failed nodes don't interfere with service operation.

## Development

### Project Structure

```
pulseha/
├── cmd/                    # Command-line interface
├── internal/
│   ├── client/            # Client implementation
│   ├── membership/        # Cluster membership management
│   ├── server/            # Server implementation
│   └── cli/               # CLI commands
├── packages/
│   ├── config/            # Configuration management
│   ├── security/          # TLS and authentication
│   ├── network/           # Network operations
│   └── utils/             # Utility functions
├── rpc/                   # gRPC definitions
└── tests/                 # Integration and unit tests
```

## Testing Framework

PulseHA includes a comprehensive testing framework that allows for thorough testing of cluster functionality without requiring root privileges or actual network interfaces.

### Test Utilities

The `tests/testutils` package provides a robust framework for simulating PulseHA clusters:

- **TestCluster**: Simulates a complete PulseHA cluster environment
- **TestNode**: Represents individual nodes in the test cluster with full functionality
- **Status Simulation**: Allows direct control of node statuses for testing failover scenarios
- **Group Management**: Supports creating, modifying, and assigning IP groups
- **Failover Testing**: Enables testing of IP failover between nodes

### Integration Tests

Integration tests in the `tests/integration` directory verify the core functionality of PulseHA:

- **Group Management**: Tests creating, modifying, and assigning IP groups
- **Failover Scenarios**: Tests IP failover between nodes in various configurations
- **Node Joining**: Tests nodes joining and leaving the cluster
- **Configuration Sync**: Tests synchronization of configuration between nodes

### Running Tests

```bash
# Run all tests
go test ./...

# Run specific integration tests
go test ./tests/integration/groups_test.go

# Run a specific test function
go test ./tests/integration/groups_test.go -run TestGroupManagement
```

### Test Environment

The test environment uses:

- In-memory configuration instead of disk files
- Localhost networking to avoid network interface requirements
- Status simulation to avoid actual network interface manipulation
- Environment variables to bypass hostname validation and other production checks

### Non-Root Testing

Most tests can run without root privileges by:

- Setting the `PULSEHA_TEST` environment variable
- Using the test utilities that simulate network operations
- Bypassing actual IP assignment operations

### Recent Improvements

The testing framework has been enhanced with several key improvements:

- **Simplified Failover Testing**: Direct control of node statuses allows for reliable testing of failover scenarios without complex network operations
- **Enhanced GetActiveIPs Method**: Improved IP tracking during failover to ensure proper IP redistribution
- **Cluster Cleanup**: Automatic cleanup of test resources to prevent interference between tests
- **Timeout Management**: Better handling of test timeouts to prevent long-running tests
- **Status Synchronization**: Improved synchronization of node statuses across the cluster during tests
- **Group Management Testing**: Enhanced testing of IP group creation, assignment, and failover

These improvements make the tests more reliable, faster, and able to run without root privileges in most cases.

### Docker Test Environment

A Docker-based test environment is now available for real-world testing of PulseHA:

- **Realistic Environment**: Tests run in Docker containers with real network interfaces and IP assignments
- **Automated Test Scripts**: Pre-configured scripts for testing failover and network partition scenarios
- **No Physical Hardware Required**: Test complex HA scenarios on a single machine
- **Root-Equivalent Testing**: Docker provides the necessary privileges without requiring root on the host
- **Network Simulation**: Test network partitions and other failure scenarios

To use the Docker test environment:

```bash
cd docker/test
./run-tests.sh
```

For more details, see the [Docker Test Environment README](docker/test/README.md).

## Contributing

Contributions are welcome! Please feel free to submit pull requests.

## License

This project is licensed under the GNU Affero General Public License v3.0 - see the LICENSE file for details.

## Support

For issues, questions, or contributions, please visit our [GitHub repository](https://github.com/syleron/pulseha).
