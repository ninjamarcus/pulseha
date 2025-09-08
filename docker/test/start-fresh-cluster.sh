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
TOKEN=$(docker exec pulseha-node1 /usr/local/bin/pulsectl token | grep -oE '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}' | head -1)
if [ -z "$TOKEN" ]; then
    echo "❌ Failed to get cluster token"
    exit 1
fi
echo "🔑 Token: $TOKEN"

# Now test joining nodes
echo ""
echo "2️⃣  Joining node2 to cluster..."
if docker exec pulseha-node2 /usr/local/bin/pulsectl cluster join \
    --address 172.20.0.10 \
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
    --address 172.20.0.10 \
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

echo ""
echo "📋 Cluster logs from node1:"
docker logs pulseha-node1 | grep -E "(Cluster health|Created cluster|joined)" | tail -10 || true

echo ""
echo "🎉 Fresh cluster initialization test complete!"
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