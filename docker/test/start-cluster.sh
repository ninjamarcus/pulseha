#!/bin/bash

set -e

echo "🚀 Starting PulseHA 3-node cluster..."

# Stop any existing cluster
echo "🛑 Stopping existing cluster..."
docker-compose -f docker-compose-fixed.yml down 2>/dev/null || true

# Start the cluster
echo "▶️  Starting containers..."
docker-compose -f docker-compose-fixed.yml up -d

# Wait for containers to be ready
echo "⏳ Waiting for containers to initialize..."
sleep 15

# Wait for services to be ready
echo "🔍 Waiting for PulseHA services to start..."
for i in {1..30}; do
    if docker exec pulseha-node1 /usr/local/bin/pulsectl status >/dev/null 2>&1; then
        echo "✅ Node 1 is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Node 1 failed to start"
        exit 1
    fi
    echo "   Attempt $i/30..."
    sleep 2
done

# Create cluster
echo "1️⃣ Creating cluster on node1..."
docker exec pulseha-node1 /usr/local/bin/pulsectl cluster create --bind-ip 172.20.0.10 --bind-port 8080 2>/dev/null &
CREATE_PID=$!
sleep 15
kill $CREATE_PID 2>/dev/null || true

# Extract token from logs - try multiple patterns
echo "🔑 Getting cluster token from logs..."
sleep 5
TOKEN=$(docker logs pulseha-node1 2>&1 | grep -E "(token:|Generated cluster token)" | tail -1 | sed -E 's/.*token:? ?//g' | tr -d '"' | tr -d '\r\n' | xargs)

# If no token found, try getting it directly
if [ -z "$TOKEN" ]; then
    echo "🔍 Trying to get token directly..."
    TOKEN=$(docker exec pulseha-node1 /usr/local/bin/pulsectl cluster token 2>/dev/null | head -1 | tr -d '\r\n' | xargs) || true
fi

if [ -z "$TOKEN" ]; then
    echo "❌ Could not extract cluster token"
    echo "📋 Node1 recent logs:"
    docker logs pulseha-node1 2>&1 | tail -20
    echo ""
    echo "🔍 Trying to list what's in the logs..."
    docker logs pulseha-node1 2>&1 | grep -i token || echo "No token found in logs"
    exit 1
fi

echo "🔑 Using token: [$TOKEN]"

# Add nodes to cluster
echo "2️⃣ Adding node2 to cluster..."
docker exec pulseha-node2 /usr/local/bin/pulsectl cluster join --address 172.20.0.10:8080 --token "$TOKEN" --bind-ip 172.20.0.11 --bind-port 8080 &
JOIN2_PID=$!
sleep 15
kill $JOIN2_PID 2>/dev/null || true
echo "Node2 join attempt completed"

echo "3️⃣ Adding node3 to cluster..."
docker exec pulseha-node3 /usr/local/bin/pulsectl cluster join --address 172.20.0.10:8080 --token "$TOKEN" --bind-ip 172.20.0.12 --bind-port 8080 &
JOIN3_PID=$!
sleep 15  
kill $JOIN3_PID 2>/dev/null || true
echo "Node3 join attempt completed"

echo "4️⃣ Checking cluster status..."
sleep 5

echo "🎯 Showing the improved cluster health logging:"
echo "----------------------------------------"
docker logs pulseha-node1 2>&1 | grep "Cluster health" | tail -3
echo "----------------------------------------"

echo ""
echo "🔍 Testing status command..."
docker exec pulseha-node1 /usr/local/bin/pulsectl status &
STATUS_PID=$!
sleep 10
kill $STATUS_PID 2>/dev/null || true
echo "⚠️  Status command completed (may have timed out)"

echo ""
echo "🔍 Checking what nodes are actually listening on:"
echo "Node 1:" && docker exec pulseha-node1 netstat -tlnp 2>/dev/null | grep :8080 || echo "  Not listening"
echo "Node 2:" && docker exec pulseha-node2 netstat -tlnp 2>/dev/null | grep :8080 || echo "  Not listening"
echo "Node 3:" && docker exec pulseha-node3 netstat -tlnp 2>/dev/null | grep :8080 || echo "  Not listening"

echo ""
echo "🎉 3-node PulseHA cluster is ready!"
echo ""
echo "📋 Commands to use:"
echo "  View improved health logs: docker logs -f pulseha-node1 | grep 'Cluster health'"
echo "  View all logs:            docker logs -f pulseha-node1"
echo "  Status:                   docker exec pulseha-node1 /usr/local/bin/pulsectl status"
echo "  Stop:                     docker-compose -f docker-compose-fixed.yml down"
echo ""
echo "✨ Look for the improved logging format like:"
echo "   time=\"2025-08-18T14:50:01Z\" level=info msg=\"Cluster health: node1(local/Active), node2(0.25ms/Passive), node3(0.13ms/Passive)\""