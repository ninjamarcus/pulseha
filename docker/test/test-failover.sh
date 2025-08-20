#!/bin/bash

set -e

echo "🧪 Testing PulseHA failover scenarios..."

# Function to run pulseha command in container
run_pulseha() {
    local container=$1
    shift
    docker exec "$container" /usr/local/bin/pulseha "$@"
}

# Function to check if IP is assigned to a node
check_ip_assignment() {
    local ip=$1
    local expected_node=$2
    
    for node in pulseha-node1 pulseha-node2 pulseha-node3; do
        if docker exec "$node" ip addr show eth0 2>/dev/null | grep -q "$ip"; then
            if [ "$node" = "$expected_node" ]; then
                echo "✅ IP $ip is correctly assigned to $node"
                return 0
            else
                echo "❌ IP $ip is assigned to $node but expected on $expected_node"
                return 1
            fi
        fi
    done
    echo "⚠️  IP $ip is not assigned to any node"
    return 1
}

echo "1️⃣ Initial cluster status..."
run_pulseha pulseha-node1 status

echo ""
echo "2️⃣ Testing node1 failure simulation..."
echo "   Stopping node1..."
docker stop pulseha-node1

echo "   Waiting for failover to occur..."
sleep 10

echo "   Checking cluster status from node2..."
if run_pulseha pulseha-node2 status; then
    echo "✅ Cluster is still operational after node1 failure"
else
    echo "❌ Cluster is not operational after node1 failure"
fi

echo ""
echo "3️⃣ Testing node1 recovery..."
echo "   Starting node1..."
docker start pulseha-node1

echo "   Waiting for node1 to rejoin..."
sleep 15

echo "   Checking cluster status..."
run_pulseha pulseha-node1 status

echo ""
echo "4️⃣ Testing IP group status..."
run_pulseha pulseha-node1 group list

echo ""
echo "5️⃣ Testing network connectivity..."
echo "   Checking if nodes can reach each other..."

for node in pulseha-node1 pulseha-node2 pulseha-node3; do
    if docker exec "$node" ping -c 2 172.20.0.10 >/dev/null 2>&1; then
        echo "✅ $node can reach node1 (172.20.0.10)"
    else
        echo "❌ $node cannot reach node1 (172.20.0.10)"
    fi
done

echo ""
echo "🎯 Failover test complete!"
echo ""
echo "Additional manual tests you can run:"
echo "  # Test active-active mode:"
echo "  docker exec pulseha-node1 /usr/local/bin/pulseha cluster mode set --mode active-active"
echo ""
echo "  # Manually promote a node:"
echo "  docker exec pulseha-node1 /usr/local/bin/pulseha node promote --node node2"
echo ""
echo "  # Check logs:"
echo "  docker logs pulseha-node1"
echo "  docker logs pulseha-node2"
echo "  docker logs pulseha-node3"
echo ""