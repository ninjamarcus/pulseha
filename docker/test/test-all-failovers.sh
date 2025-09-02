#!/bin/bash

echo "=== Testing Failover for All Nodes ==="
echo ""

# Function to monitor logs from all passive nodes
monitor_logs() {
    echo "Starting log monitoring..."
    docker logs -f pulseha-node2 2>&1 | grep -E "Cluster health|Status change|election|Active node|failover" | sed 's/^/[node2] /' &
    PID2=$!
    docker logs -f pulseha-node3 2>&1 | grep -E "Cluster health|Status change|election|Active node|failover" | sed 's/^/[node3] /' &
    PID3=$!
    echo $PID2 > /tmp/monitor2.pid
    echo $PID3 > /tmp/monitor3.pid
}

# Function to stop monitoring
stop_monitoring() {
    if [ -f /tmp/monitor2.pid ]; then
        kill $(cat /tmp/monitor2.pid) 2>/dev/null
        rm /tmp/monitor2.pid
    fi
    if [ -f /tmp/monitor3.pid ]; then
        kill $(cat /tmp/monitor3.pid) 2>/dev/null
        rm /tmp/monitor3.pid
    fi
}

# Test 1: Fail node1 (Active)
echo "Test 1: Pausing node1 (currently Active)"
echo "----------------------------------------"
monitor_logs
sleep 2
docker pause pulseha-node1
echo "Waiting 10 seconds for failover..."
sleep 10
stop_monitoring
echo ""
echo "Checking who became active..."
docker logs pulseha-node2 --tail 3 2>&1 | grep -E "Cluster|Active"
docker logs pulseha-node3 --tail 3 2>&1 | grep -E "Cluster|Active"
echo ""

# Resume node1
echo "Resuming node1..."
docker unpause pulseha-node1
sleep 5

echo "Press Enter to continue to Test 2..."
read

# Check who is active now
echo "Current cluster state:"
docker logs pulseha-node2 --tail 1 2>&1 | grep "Cluster"
docker logs pulseha-node3 --tail 1 2>&1 | grep "Cluster"
echo ""

echo "Test complete!"
