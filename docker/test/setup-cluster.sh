#!/bin/bash

set -e

echo "🚀 Setting up PulseHA test cluster..."

# Wait for containers to be ready
echo "⏳ Waiting for containers to start..."
sleep 30

# Function to run pulseha command in container
run_pulseha() {
    local container=$1
    shift
    docker exec "$container" /usr/local/bin/pulseha "$@"
}

# Function to wait for service to be ready
wait_for_service() {
    local container=$1
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Waiting for $container to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker exec "$container" /usr/local/bin/pulseha status >/dev/null 2>&1; then
            echo "✅ $container is ready"
            return 0
        fi
        echo "   Attempt $attempt/$max_attempts failed, retrying..."
        sleep 2
        ((attempt++))
    done
    echo "❌ $container failed to start after $max_attempts attempts"
    return 1
}

echo "1️⃣ Creating cluster on node1..."
if ! run_pulseha pulseha-node1 cluster create --bind-ip 172.20.0.10; then
    echo "⚠️  Cluster already exists or creation failed, continuing..."
fi

# Get cluster token
echo "🔑 Getting cluster token..."
TOKEN=$(docker exec pulseha-node1 /usr/local/bin/pulseha cluster token 2>/dev/null || echo "")

if [ -z "$TOKEN" ]; then
    echo "⚠️  Could not get cluster token, cluster might need to be recreated"
else
    echo "2️⃣ Adding node2 to cluster..."
    run_pulseha pulseha-node2 cluster join --address 172.20.0.10:8080 --token "$TOKEN" --bind-ip 172.20.0.11

    echo "3️⃣ Adding node3 to cluster..."
    run_pulseha pulseha-node3 cluster join --address 172.20.0.10:8080 --token "$TOKEN" --bind-ip 172.20.0.12
fi

echo "4️⃣ Creating test IP group..."
run_pulseha pulseha-node1 group create --name test-ips

echo "5️⃣ Assigning group to nodes..."
run_pulseha pulseha-node1 group assign --group test-ips --node node1 --interface eth0
run_pulseha pulseha-node1 group assign --group test-ips --node node2 --interface eth0
run_pulseha pulseha-node1 group assign --group test-ips --node node3 --interface eth0

echo "6️⃣ Adding floating IPs..."
run_pulseha pulseha-node1 group add-ip --group test-ips --ip 172.20.100.10/24
run_pulseha pulseha-node1 group add-ip --group test-ips --ip 172.20.100.11/24

echo "7️⃣ Checking cluster status..."
run_pulseha pulseha-node1 status

echo "8️⃣ Listing IP groups..."
run_pulseha pulseha-node1 group list

echo ""
echo "🎉 PulseHA test cluster setup complete!"
echo ""
echo "You can now test the cluster with:"
echo "  docker exec -it pulseha-node1 /usr/local/bin/pulseha status"
echo "  docker exec -it pulseha-node1 /usr/local/bin/pulseha group list"
echo ""
echo "To test failover:"
echo "  docker stop pulseha-node1"
echo "  docker exec -it pulseha-node2 /usr/local/bin/pulseha status"
echo ""