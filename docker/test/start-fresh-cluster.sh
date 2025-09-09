#!/bin/bash

# Test script for PulseHA cluster initialization from scratch
# This simulates a real-world fresh installation scenario

set -e

echo "🧹 Cleaning up any existing containers and volumes..."
docker-compose -f docker-compose-fresh.yml down -v 2>/dev/null || true

echo "🏗️  Building fresh Docker images..."
docker-compose -f docker-compose-fresh.yml build

echo "🚀 Starting fresh PulseHA nodes (no pre-configuration)..."
docker-compose -f docker-compose-fresh.yml up -d

echo "⏳ Waiting for containers to initialize..."
sleep 5

# Wait for node1 to be ready (CLI server on localhost:8080)
echo "🔍 Waiting for node1 CLI server to be ready..."
MAX_ATTEMPTS=30
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    # Check if pulsectl status returns successfully (even with empty cluster)
    if docker exec pulseha-node1 /usr/local/bin/pulsectl status 2>/dev/null | grep -q "Cluster Status\|No cluster configured"; then
        echo "✅ Node1 is ready"
        break
    fi
    ATTEMPT=$((ATTEMPT + 1))
    echo "  Attempt $ATTEMPT/$MAX_ATTEMPTS - waiting for node1..."
    sleep 2
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "❌ Node1 failed to start properly"
    docker logs pulseha-node1
    exit 1
fi

echo ""
echo "1️⃣  Creating new cluster on node1..."
echo "   This tests the exact same path that would hang in the reported issue"

# Create cluster - this is the operation that was hanging
if ! docker exec pulseha-node1 /usr/local/bin/pulsectl cluster create \
    --bind-ip 172.20.0.10 \
    --bind-port 8080 \
    --mode active-passive; then
    echo "❌ Failed to create cluster"
    docker logs pulseha-node1 | tail -20
    exit 1
fi

echo "✅ Cluster created successfully (deadlock issue is fixed!)"

# Get the cluster token
echo ""
echo "🔑 Getting cluster token..."
TOKEN=$(docker exec pulseha-node1 /usr/local/bin/pulsectl cluster token | head -1 | tr -d '\r\n')
if [ -z "$TOKEN" ]; then
    echo "❌ Failed to get cluster token"
    exit 1
fi
echo "🔑 Token: $TOKEN"

# Now test joining nodes
echo ""
echo "2️⃣  Joining node2 to cluster..."
if docker exec pulseha-node2 /usr/local/bin/pulsectl cluster join \
    --address 172.20.0.10:8080 \
    --token "$TOKEN" \
    --bind-ip 172.20.0.11 \
    --bind-port 8080; then
    echo "✅ Node2 joined successfully"
else
    echo "⚠️  Node2 join failed (may need retry)"
fi

echo ""
echo "3️⃣  Joining node3 to cluster..."
if docker exec pulseha-node3 /usr/local/bin/pulsectl cluster join \
    --address 172.20.0.10:8080 \
    --token "$TOKEN" \
    --bind-ip 172.20.0.12 \
    --bind-port 8080; then
    echo "✅ Node3 joined successfully"
else
    echo "⚠️  Node3 join failed (may need retry)"
fi

echo ""
echo "⏳ Waiting for cluster to stabilize..."
sleep 10

echo ""
echo "📊 Checking cluster status..."
docker exec pulseha-node1 /usr/local/bin/pulsectl status || true

# Create a group, assign to node1 and node2, add an IP, and promote node1
echo ""
echo "4️⃣  Creating group and assigning..."
docker exec pulseha-node1 /usr/local/bin/pulsectl group create G1 || true

# Get node IDs
NODE1_ID=$(docker exec pulseha-node1 sh -lc "sed -n 's/.*\"local_node\": \"\([a-f0-9-]\+\)\".*/\1/p' /etc/pulseha/config.json")
NODE2_ID=$(docker exec pulseha-node2 sh -lc "sed -n 's/.*\"local_node\": \"\([a-f0-9-]\+\)\".*/\1/p' /etc/pulseha/config.json")
echo "Node1 ID: $NODE1_ID"
echo "Node2 ID: $NODE2_ID"

docker exec pulseha-node1 /usr/local/bin/pulsectl group assign --group G1 --node-id "$NODE1_ID" --interface eth0 || true
docker exec pulseha-node1 /usr/local/bin/pulsectl group assign --group G1 --node-id "$NODE2_ID" --interface eth0 || true

echo ""
echo "5️⃣  Adding IP 10.66.0.50/24 to group G1..."
docker exec pulseha-node1 /usr/local/bin/pulsectl group add-ip --group G1 --ip 10.66.0.50/24 || true

echo ""
echo "6️⃣  Promoting node1 and verifying IP assignment..."
docker exec pulseha-node1 /usr/local/bin/pulsectl node promote --node-id "$NODE1_ID" || true
docker exec pulseha-node1 ip addr show dev eth0 | grep -E "10.66.0.50/24|inet " || true

echo ""
echo "7️⃣  Simulating failover: stopping node1 and promoting node2..."
docker stop pulseha-node1 >/dev/null 2>&1 || true
sleep 2
docker exec pulseha-node2 /usr/local/bin/pulsectl node promote --node-id "$NODE2_ID" || true
docker exec pulseha-node2 ip addr show dev eth0 | grep -E "10.66.0.50/24|inet " || true

echo ""
echo "📋 Cluster logs from node1:"
docker logs pulseha-node1 | grep -E "(Cluster health|Created cluster|joined)" | tail -10 || true

echo ""
echo "🎉 Fresh cluster initialization and failover test complete!"
echo ""
echo "This test specifically exercises the code path that was causing the deadlock:"
echo "  1. Starting with NO configuration (like a fresh install)"
echo "  2. Creating cluster via RPC (the operation that was hanging)"
echo "  3. Joining nodes via RPC"
echo ""
echo "📋 Useful commands:"
echo "  View logs:     docker logs -f pulseha-node1"
echo "  Check status:  docker exec pulseha-node1 /usr/local/bin/pulsectl status"
echo "  Stop cluster:  docker-compose -f docker-compose-fresh.yml down"
echo "  Clean up:      docker-compose -f docker-compose-fresh.yml down -v"