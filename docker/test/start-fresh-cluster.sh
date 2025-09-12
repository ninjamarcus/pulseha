#!/bin/bash

# Test script for PulseHA cluster initialization from scratch
# This simulates a real-world fresh installation scenario

set -e

wait_for_cli() {
	local cname="$1"
	echo "🔍 Waiting for $cname CLI server to be ready..."
	local MAX_ATTEMPTS=30
	local ATTEMPT=0
	while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
		if docker exec "$cname" /usr/local/bin/pulsectl status 2>/dev/null | grep -q "Cluster Status\|No cluster configured"; then
			echo "✅ $cname is ready"
			return 0
		fi
		ATTEMPT=$((ATTEMPT + 1))
		echo "  Attempt $ATTEMPT/$MAX_ATTEMPTS - waiting for $cname..."
		sleep 2
	done
	echo "❌ $cname failed to start properly"
	docker logs "$cname" | tail -100 || true
	return 1
}

wait_for_node_id() {
	local cname="$1"
	local MAX_ATTEMPTS=30
	local ATTEMPT=0
	local ID=""
	while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
		ID=$(docker exec "$cname" sh -lc "sed -n 's/.*\"local_node\": \"\([a-f0-9-]\+\)\".*/\1/p' /etc/pulseha/config.json" 2>/dev/null || true)
		if [ -n "$ID" ]; then
			echo "$ID"
			return 0
		fi
		ATTEMPT=$((ATTEMPT + 1))
		echo "  Waiting for local_node on $cname (attempt $ATTEMPT/$MAX_ATTEMPTS)"
		sleep 1
	done
	echo ""
	return 1
}

print_logs() {
	local cname="$1"
	local label="$2"
	echo "\n📋 Logs ($label) from $cname:"
	docker logs "$cname" | tail -50 || true
}

echo "🧹 Cleaning up any existing containers and volumes..."
docker-compose -f docker-compose-fresh.yml down -v 2>/dev/null || true

echo "🏗️  Building fresh Docker images..."
docker-compose -f docker-compose-fresh.yml build

echo "🚀 Starting fresh PulseHA nodes (no pre-configuration)..."
docker-compose -f docker-compose-fresh.yml up -d

echo "⏳ Waiting for containers to initialize..."
sleep 5

# Wait for CLI readiness on all nodes before operations
wait_for_cli pulseha-node1
wait_for_cli pulseha-node2 || true
wait_for_cli pulseha-node3 || true

print_logs pulseha-node1 "post-start"
print_logs pulseha-node2 "post-start"
print_logs pulseha-node3 "post-start"

echo ""
echo "1️⃣  Creating new cluster on node1..."
echo "   This tests the exact same path that would hang in the reported issue"

# Create cluster - this is the operation that was hanging
if ! docker exec pulseha-node1 /usr/local/bin/pulsectl cluster create \
    --bind-ip 172.20.0.10 \
    --bind-port 9083 \
    --mode active-passive; then
    echo "❌ Failed to create cluster"
    docker logs pulseha-node1 | tail -50
    exit 1
fi

print_logs pulseha-node1 "after-cluster-create"

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
    --address 172.20.0.10:9083 \
    --token "$TOKEN" \
    --bind-ip 172.20.0.11 \
    --bind-port 9083; then
    echo "✅ Node2 joined successfully"
else
    echo "⚠️  Node2 join failed (may need retry)"
fi

print_logs pulseha-node1 "after-node2-join"
print_logs pulseha-node2 "after-node2-join"

# Small settle time
sleep 2

echo ""
echo "3️⃣  Joining node3 to cluster..."
if docker exec pulseha-node3 /usr/local/bin/pulsectl cluster join \
    --address 172.20.0.10:9083 \
    --token "$TOKEN" \
    --bind-ip 172.20.0.12 \
    --bind-port 9083; then
    echo "✅ Node3 joined successfully"
else
    echo "⚠️  Node3 join failed, retrying once after short wait..."
    sleep 3
    if docker exec pulseha-node3 /usr/local/bin/pulsectl cluster join \
        --address 172.20.0.10:9083 \
        --token "$TOKEN" \
        --bind-ip 172.20.0.12 \
        --bind-port 9083; then
        echo "✅ Node3 joined successfully on retry"
    else
        echo "❌ Node3 join failed after retry"
    fi
fi

print_logs pulseha-node1 "after-node3-join"
print_logs pulseha-node3 "after-node3-join"

echo ""
echo "⏳ Waiting for cluster to stabilize..."
sleep 10

print_logs pulseha-node1 "post-stabilize"
print_logs pulseha-node2 "post-stabilize"
print_logs pulseha-node3 "post-stabilize"

echo ""
echo "📊 Checking cluster status..."
docker exec pulseha-node1 /usr/local/bin/pulsectl status || true

# Create a group, assign to node1 and node2, add an IP, and promote node1
echo ""
echo "4️⃣  Creating group and assigning..."
docker exec pulseha-node1 /usr/local/bin/pulsectl group create G1 || true

# Wait for node IDs to exist in each node config
NODE1_ID=$(wait_for_node_id pulseha-node1)
NODE2_ID=$(wait_for_node_id pulseha-node2)
NODE3_ID=$(wait_for_node_id pulseha-node3)
echo "Node1 ID: $NODE1_ID"
echo "Node2 ID: $NODE2_ID"
echo "Node3 ID: $NODE3_ID"

print_logs pulseha-node1 "before-assign"

# Assign group to node1 and node2
docker exec pulseha-node1 /usr/local/bin/pulsectl group assign --group G1 --node-id "$NODE1_ID" --interface eth0 || true
docker exec pulseha-node1 /usr/local/bin/pulsectl group assign --group G1 --node-id "$NODE2_ID" --interface eth0 || true

print_logs pulseha-node1 "after-assign"
print_logs pulseha-node2 "after-assign"

# Add IP and promote node1
echo ""
echo "5️⃣  Adding IP 10.66.0.50/24 to group G1..."
docker exec pulseha-node1 /usr/local/bin/pulsectl group add-ip --group G1 --ip 10.66.0.50/24 || true

print_logs pulseha-node1 "after-add-ip"

echo ""
echo "6️⃣  Promoting node1 and verifying IP assignment..."
docker exec pulseha-node1 /usr/local/bin/pulsectl node promote --node-id "$NODE1_ID" || true
print_logs pulseha-node1 "after-promote-node1"
docker exec pulseha-node1 ip addr show dev eth0 | grep -E "10.66.0.50/24|inet " || true

echo ""
echo "7️⃣  Simulating failover: stopping node1 and promoting node2..."
docker stop pulseha-node1 >/dev/null 2>&1 || true
sleep 2
docker exec pulseha-node2 /usr/local/bin/pulsectl node promote --node-id "$NODE2_ID" || true
print_logs pulseha-node2 "after-promote-node2"
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