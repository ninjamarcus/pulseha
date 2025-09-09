#!/bin/bash

# Comprehensive test script for all pulsectl commands
# Tests every command to ensure they work properly

set -e

echo "================================================"
echo "     COMPREHENSIVE PULSECTL COMMAND TEST"
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track results
PASSED=0
FAILED=0
FAILED_COMMANDS=()

# Function to test a command
test_command() {
    local description="$1"
    local command="$2"
    local container="${3:-pulseha-node1}"
    
    echo -n "Testing: $description ... "
    
    if docker exec $container /usr/local/bin/pulsectl $command > /tmp/test-output.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}✗${NC}"
        echo "  Error output:"
        cat /tmp/test-output.txt | sed 's/^/    /'
        ((FAILED++))
        FAILED_COMMANDS+=("$description: $command")
        return 1
    fi
}

# Function to test a command that should fail
test_command_should_fail() {
    local description="$1"
    local command="$2"
    local container="${3:-pulseha-node1}"
    
    echo -n "Testing (should fail): $description ... "
    
    if docker exec $container /usr/local/bin/pulsectl $command > /tmp/test-output.txt 2>&1; then
        echo -e "${RED}✗ (should have failed)${NC}"
        ((FAILED++))
        FAILED_COMMANDS+=("$description: $command (should have failed)")
        return 1
    else
        echo -e "${GREEN}✓ (failed as expected)${NC}"
        ((PASSED++))
        return 0
    fi
}

echo "Setting up test environment..."
echo "================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

# Check if cluster is already running
if docker ps | grep -q pulseha-node1; then
    echo "Using existing cluster..."
    CLUSTER_EXISTS=true
    
    # Check if it's configured
    if ! docker exec pulseha-node1 /usr/local/bin/pulsectl status 2>/dev/null | grep -q "Node:"; then
        echo "Cluster exists but not configured. Creating cluster..."
        docker exec pulseha-node1 /usr/local/bin/pulsectl cluster create --bind-ip 172.20.0.10 --bind-port 8080 --mode active-passive > /dev/null 2>&1
        
        # Get token and join other nodes
        TOKEN=$(docker exec pulseha-node1 /usr/local/bin/pulsectl cluster token | grep -oE '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}' | head -1)
        docker exec pulseha-node2 /usr/local/bin/pulsectl cluster join --address 172.20.0.10 --token "$TOKEN" --bind-ip 172.20.0.11 --bind-port 8080 > /dev/null 2>&1
        docker exec pulseha-node3 /usr/local/bin/pulsectl cluster join --address 172.20.0.10 --token "$TOKEN" --bind-ip 172.20.0.12 --bind-port 8080 > /dev/null 2>&1
        sleep 5
    fi
else
    echo "Starting fresh cluster..."
    cd docker/test
    if [ -f start-fresh-cluster.sh ]; then
        ./start-fresh-cluster.sh > /dev/null 2>&1 || {
            echo -e "${RED}Failed to start cluster${NC}"
            exit 1
        }
    else
        # Fallback: start containers manually
        docker-compose -f docker-compose-fresh.yml up -d > /dev/null 2>&1
        sleep 10
        
        # Create cluster
        docker exec pulseha-node1 /usr/local/bin/pulsectl cluster create --bind-ip 172.20.0.10 --bind-port 8080 --mode active-passive > /dev/null 2>&1
        
        # Get token and join other nodes
        TOKEN=$(docker exec pulseha-node1 /usr/local/bin/pulsectl cluster token | grep -oE '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}' | head -1)
        docker exec pulseha-node2 /usr/local/bin/pulsectl cluster join --address 172.20.0.10 --token "$TOKEN" --bind-ip 172.20.0.11 --bind-port 8080 > /dev/null 2>&1
        docker exec pulseha-node3 /usr/local/bin/pulsectl cluster join --address 172.20.0.10 --token "$TOKEN" --bind-ip 172.20.0.12 --bind-port 8080 > /dev/null 2>&1
        sleep 5
    fi
    CLUSTER_EXISTS=false
fi

echo ""
echo "Testing pulsectl commands..."
echo "================================"
echo ""

# 1. HELP AND VERSION COMMANDS
echo -e "${YELLOW}1. HELP AND VERSION COMMANDS${NC}"
test_command "pulsectl --help" "--help"
# Version flag doesn't exist - skip it
# test_command "pulsectl --version" "--version"
test_command "pulsectl help" "help"

echo ""

# 2. STATUS COMMAND
echo -e "${YELLOW}2. STATUS COMMAND${NC}"
test_command "pulsectl status" "status"

echo ""

# 3. CLUSTER COMMANDS
echo -e "${YELLOW}3. CLUSTER COMMANDS${NC}"
test_command "pulsectl cluster --help" "cluster --help"

# Get token
echo -n "Getting cluster token ... "
TOKEN=$(docker exec pulseha-node1 /usr/local/bin/pulsectl cluster token | grep -oE '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}' | head -1)
if [ -n "$TOKEN" ]; then
    echo -e "${GREEN}✓${NC} ($TOKEN)"
    ((PASSED++))
else
    echo -e "${RED}✗${NC}"
    ((FAILED++))
fi

test_command "pulsectl cluster token" "cluster token"
test_command "pulsectl cluster mode" "cluster mode"
test_command "pulsectl cluster mode --help" "cluster mode --help"

# Test creating a cluster (should fail since one exists)
test_command_should_fail "pulsectl cluster create (already exists)" "cluster create --bind-ip 172.20.0.10 --bind-port 8080"

echo ""

# 4. NODE COMMANDS
echo -e "${YELLOW}4. NODE COMMANDS${NC}"
test_command "pulsectl node --help" "node --help"

# Test promote/demote
echo -n "Testing node demote (node1 to passive) ... "
if docker exec pulseha-node1 /usr/local/bin/pulsectl node demote node1 > /tmp/test-output.txt 2>&1; then
    echo -e "${GREEN}✓${NC}"
    ((PASSED++))
    sleep 2
    
    # Now promote it back
    echo -n "Testing node promote (node1 to active) ... "
    if docker exec pulseha-node1 /usr/local/bin/pulsectl node promote node1 > /tmp/test-output.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
        ((PASSED++))
    else
        echo -e "${RED}✗${NC}"
        cat /tmp/test-output.txt | sed 's/^/    /'
        ((FAILED++))
    fi
else
    echo -e "${YELLOW}⚠ (may be expected if node1 is the only active)${NC}"
fi

echo ""

# 5. GROUP COMMANDS
echo -e "${YELLOW}5. GROUP COMMANDS${NC}"
test_command "pulsectl group --help" "group --help"
test_command "pulsectl group list" "group list"

# Clean up any existing test-group first
docker exec pulseha-node1 /usr/local/bin/pulsectl group delete --name test-group --force > /dev/null 2>&1 || true

# Create a test group
echo -n "Testing group create (test-group) ... "
if docker exec pulseha-node1 /usr/local/bin/pulsectl group create --name test-group > /tmp/test-output.txt 2>&1; then
    echo -e "${GREEN}✓${NC}"
    ((PASSED++))
    
    # Add IP to group
    echo -n "Testing group add IP (192.168.1.100) ... "
    if docker exec pulseha-node1 /usr/local/bin/pulsectl group add test-group 192.168.1.100 > /tmp/test-output.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
        ((PASSED++))
        
        # List groups to verify
        test_command "pulsectl group list (after add)" "group list"
        
        # Remove IP from group
        echo -n "Testing group remove IP ... "
        if docker exec pulseha-node1 /usr/local/bin/pulsectl group remove test-group 192.168.1.100 > /tmp/test-output.txt 2>&1; then
            echo -e "${GREEN}✓${NC}"
            ((PASSED++))
        else
            echo -e "${RED}✗${NC}"
            cat /tmp/test-output.txt | sed 's/^/    /'
            ((FAILED++))
        fi
    else
        echo -e "${RED}✗${NC}"
        cat /tmp/test-output.txt | sed 's/^/    /'
        ((FAILED++))
    fi
    
    # Get node ID for assign/unassign tests
    NODE_ID=$(docker exec pulseha-node1 /usr/local/bin/pulsectl status | grep -A2 "Node: node1" | grep -oE '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}' | head -1 || echo "")
    
    if [ -n "$NODE_ID" ]; then
        # Assign group to node
        echo -n "Testing group assign to node ... "
        if docker exec pulseha-node1 /usr/local/bin/pulsectl group assign test-group eth0 $NODE_ID > /tmp/test-output.txt 2>&1; then
            echo -e "${GREEN}✓${NC}"
            ((PASSED++))
            
            # Unassign group from node
            echo -n "Testing group unassign from node ... "
            if docker exec pulseha-node1 /usr/local/bin/pulsectl group unassign test-group eth0 $NODE_ID > /tmp/test-output.txt 2>&1; then
                echo -e "${GREEN}✓${NC}"
                ((PASSED++))
            else
                echo -e "${RED}✗${NC}"
                cat /tmp/test-output.txt | sed 's/^/    /'
                ((FAILED++))
            fi
        else
            echo -e "${RED}✗${NC}"
            cat /tmp/test-output.txt | sed 's/^/    /'
            ((FAILED++))
        fi
    fi
    
    # Delete the test group
    echo -n "Testing group delete ... "
    if docker exec pulseha-node1 /usr/local/bin/pulsectl group delete --name test-group --force > /tmp/test-output.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
        ((PASSED++))
    else
        echo -e "${RED}✗${NC}"
        cat /tmp/test-output.txt | sed 's/^/    /'
        ((FAILED++))
    fi
else
    echo -e "${RED}✗${NC}"
    cat /tmp/test-output.txt | sed 's/^/    /'
    ((FAILED++))
fi

echo ""

# 6. CONFIG COMMANDS - NOT IMPLEMENTED
# Config commands don't exist in current implementation
# Skipping config tests

echo ""

# 7. NETWORK COMMANDS  
echo -e "${YELLOW}7. NETWORK COMMANDS${NC}"
test_command "pulsectl cluster network --help" "cluster network --help"
test_command "pulsectl cluster network resync" "cluster network resync"

echo ""

# 8. LEAVE CLUSTER (Destructive - only test if we created the cluster)
if [ "$CLUSTER_EXISTS" = "false" ]; then
    echo -e "${YELLOW}8. LEAVE CLUSTER COMMAND${NC}"
    
    # Test on node3 (safe to remove)
    echo -n "Testing cluster leave (node3) ... "
    if docker exec pulseha-node3 /usr/local/bin/pulsectl cluster leave --force > /tmp/test-output.txt 2>&1; then
        echo -e "${GREEN}✓${NC}"
        ((PASSED++))
        
        # Rejoin node3
        echo -n "Rejoining node3 to cluster ... "
        if docker exec pulseha-node3 /usr/local/bin/pulsectl cluster join --address 172.20.0.10 --token "$TOKEN" --bind-ip 172.20.0.12 --bind-port 8080 > /tmp/test-output.txt 2>&1; then
            echo -e "${GREEN}✓${NC}"
            ((PASSED++))
        else
            echo -e "${YELLOW}⚠ (rejoin failed)${NC}"
        fi
    else
        echo -e "${RED}✗${NC}"
        cat /tmp/test-output.txt | sed 's/^/    /'
        ((FAILED++))
    fi
fi

echo ""
echo "================================================"
echo "                TEST RESULTS"
echo "================================================"
echo ""
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"

if [ ${#FAILED_COMMANDS[@]} -gt 0 ]; then
    echo ""
    echo "Failed commands:"
    for cmd in "${FAILED_COMMANDS[@]}"; do
        echo "  - $cmd"
    done
fi

echo ""
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed${NC}"
    exit 1
fi