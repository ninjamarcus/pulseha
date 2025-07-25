# PulseHA Docker Test Environment

This directory contains a complete Docker-based test environment for PulseHA that allows you to test cluster functionality, failover scenarios, and floating IP management without affecting your host system.

## Features

- **Multi-node cluster**: 3 PulseHA nodes running in separate containers
- **Realistic networking**: Dedicated bridge network with static IP addresses
- **Privileged containers**: Full network control for IP management
- **Automated setup**: Scripts to initialize cluster and test failover
- **Health monitoring**: Built-in health checks for all nodes

## Quick Start

1. **Build and start the cluster:**
   ```bash
   cd docker/test
   docker-compose up -d
   ```

2. **Setup the cluster:**
   ```bash
   ./setup-cluster.sh
   ```

3. **Test failover scenarios:**
   ```bash
   ./test-failover.sh
   ```

## Architecture

The test environment consists of:

- **3 PulseHA nodes**:
  - `pulseha-node1` (172.20.0.10:8080) - Primary node
  - `pulseha-node2` (172.20.0.11:8080) - Secondary node  
  - `pulseha-node3` (172.20.0.12:8080) - Tertiary node

- **Network**: Custom bridge network (172.20.0.0/16)
- **Floating IPs**: 172.20.100.10/24, 172.20.100.11/24

## Manual Testing

### Basic Operations

```bash
# Check cluster status
docker exec -it pulseha-node1 /usr/local/bin/pulsectl status

# List IP groups
docker exec -it pulseha-node1 /usr/local/bin/pulsectl group list

# Create a new group
docker exec -it pulseha-node1 /usr/local/bin/pulsectl group create --name web-servers

# Add IP to group
docker exec -it pulseha-node1 /usr/local/bin/pulsectl group add-ip --group web-servers --ip 172.20.100.20/24

# Assign group to node
docker exec -it pulseha-node1 /usr/local/bin/pulsectl group assign --group web-servers --node node2 --interface eth0
```

### Failover Testing

```bash
# Test node failure
docker stop pulseha-node1
docker exec -it pulseha-node2 /usr/local/bin/pulsectl status

# Test node recovery  
docker start pulseha-node1
sleep 10
docker exec -it pulseha-node1 /usr/local/bin/pulsectl status

# Test active-active mode
docker exec -it pulseha-node1 /usr/local/bin/pulsectl cluster mode set --mode active-active
```

### Network Testing

```bash
# Check if floating IPs are assigned
docker exec pulseha-node1 ip addr show eth0
docker exec pulseha-node2 ip addr show eth0
docker exec pulseha-node3 ip addr show eth0

# Test connectivity between nodes
docker exec pulseha-node1 ping -c 3 172.20.0.11
docker exec pulseha-node2 ping -c 3 172.20.0.12
```

## Troubleshooting

### View Logs
```bash
docker logs pulseha-node1
docker logs pulseha-node2  
docker logs pulseha-node3
```

### Debug Container
```bash
# Get shell access to a container
docker exec -it pulseha-node1 /bin/bash

# Check network interfaces
docker exec pulseha-node1 ip addr

# Check processes
docker exec pulseha-node1 ps aux
```

### Reset Environment
```bash
# Stop and remove everything
docker-compose down -v

# Remove images
docker rmi $(docker images "*pulseha*" -q)

# Start fresh
docker-compose up -d --build
./setup-cluster.sh
```

## Configuration

The containers use environment variables for configuration:

- `PULSEHA_NODE_ID`: Unique identifier for the node
- `PULSEHA_BIND_IP`: IP address to bind the gRPC server
- `PULSEHA_BIND_PORT`: Port for gRPC server (default: 8080)

## Limitations

- Requires Docker with privileged container support
- Network interface manipulation requires `NET_ADMIN` capability
- Some features may not work identically to bare-metal deployments

## Support

This test environment is designed to verify PulseHA's core functionality:

- ✅ Cluster creation and membership
- ✅ Node health monitoring  
- ✅ Floating IP management
- ✅ Failover and recovery
- ✅ Active-passive and active-active modes
- ✅ gRPC communication between nodes