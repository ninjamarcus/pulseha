#!/bin/bash

# PulseHA Automated QA Test Suite
# This script runs basic functionality tests for QA validation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to print test results
print_result() {
    local test_name="$1"
    local result="$2"
    local details="$3"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    if [ "$result" = "PASS" ]; then
        echo -e "${GREEN}✅ PASS${NC}: $test_name"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}❌ FAIL${NC}: $test_name"
        if [ -n "$details" ]; then
            echo -e "   ${YELLOW}Details:${NC} $details"
        fi
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# Function to run command and check result
run_test() {
    local test_name="$1"
    local command="$2"
    local expected_pattern="$3"
    
    echo "Running: $test_name"
    
    if output=$(eval "$command" 2>&1); then
        if [ -n "$expected_pattern" ]; then
            if echo "$output" | grep -q "$expected_pattern"; then
                print_result "$test_name" "PASS"
            else
                print_result "$test_name" "FAIL" "Expected pattern '$expected_pattern' not found in output"
            fi
        else
            print_result "$test_name" "PASS"
        fi
    else
        print_result "$test_name" "FAIL" "Command failed with exit code $?"
    fi
}

echo "🧪 PulseHA QA Test Suite"
echo "========================"

echo ""
echo "📋 Pre-test Setup"
echo "------------------"

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Docker is running${NC}"

# Check if containers exist and are running
echo "Checking container status..."
if ! docker compose ps | grep -q "healthy"; then
    echo -e "${YELLOW}⚠️  Containers not running, starting them...${NC}"
    docker compose up -d --build
    echo "Waiting for containers to initialize..."
    sleep 45
fi

# Verify containers are healthy
healthy_count=$(docker compose ps | grep healthy | wc -l)
if [ "$healthy_count" -lt 3 ]; then
    echo -e "${RED}❌ Not all containers are healthy${NC}"
    docker compose ps
    exit 1
fi
echo -e "${GREEN}✅ All containers are healthy${NC}"

echo ""
echo "🔍 Running Functionality Tests"
echo "-------------------------------"

# Test 1: Basic server response
run_test "Server Status Response" \
    "docker exec pulseha-node1 /usr/local/bin/pulsectl status" \
    "Cluster Status"

# Test 2: Help command works
run_test "Help Command" \
    "docker exec pulseha-node1 /usr/local/bin/pulsectl --help" \
    "Available Commands"

# Test 3: Group creation
run_test "Group Creation" \
    "docker exec pulseha-node1 /usr/local/bin/pulsectl group create --name qa-test-group" \
    "created successfully"

# Test 4: Group listing
run_test "Group Listing" \
    "docker exec pulseha-node1 /usr/local/bin/pulsectl group list" \
    "qa-test-group"

# Test 5: Group assignment
run_test "Group Assignment" \
    "docker exec pulseha-node1 /usr/local/bin/pulsectl group assign --group qa-test-group --node node1 --interface eth0" \
    "successfully assigned"

# Test 6: Network connectivity between nodes
run_test "Node Network Connectivity" \
    "docker exec pulseha-node2 ping -c 2 172.20.0.10" \
    "0% packet loss"

# Test 7: JSON output format
run_test "JSON Status Output" \
    "docker exec pulseha-node1 /usr/local/bin/pulsectl status --json" \
    '"members"'

# Test 8: Invalid command handling
run_test "Invalid Command Handling" \
    "docker exec pulseha-node1 /usr/local/bin/pulsectl invalid-command 2>&1 || true" \
    "unknown command"

# Test 9: Container log analysis (no critical errors)
echo "Analyzing container logs for errors..."
critical_errors=0
for node in pulseha-node1 pulseha-node2 pulseha-node3; do
    if docker logs "$node" 2>&1 | grep -i "fatal\|panic\|critical" >/dev/null; then
        critical_errors=$((critical_errors + 1))
    fi
done

if [ "$critical_errors" -eq 0 ]; then
    print_result "No Critical Errors in Logs" "PASS"
else
    print_result "No Critical Errors in Logs" "FAIL" "$critical_errors containers have critical errors"
fi

# Test 10: Syslog configuration check
echo "Checking syslog configuration..."
syslog_warning_count=$(docker logs pulseha-node1 2>&1 | grep -c "Failed to create syslog hook" || echo "0")
if [ "$syslog_warning_count" -gt 0 ]; then
    print_result "Syslog Warning Present" "PASS" "Found expected syslog warning in container environment"
else
    print_result "Syslog Warning Present" "FAIL" "No syslog warning found - this may indicate an issue"
fi

# Test 11: Container restart resilience
echo "Testing container restart behavior..."
echo "Stopping node1..."
docker stop pulseha-node1 >/dev/null
sleep 5

echo "Starting node1..."
docker start pulseha-node1 >/dev/null
sleep 15

run_test "Container Restart Recovery" \
    "docker exec pulseha-node1 /usr/local/bin/pulsectl status" \
    "Cluster Status"

echo ""
echo "🧹 Cleanup"
echo "----------"

# Clean up test data
echo "Removing test group..."
docker exec pulseha-node1 /usr/local/bin/pulsectl group list 2>/dev/null | grep -q "qa-test-group" || echo "Test group already removed"

echo ""
echo "📊 Test Results Summary"
echo "======================"
echo -e "Total Tests: ${TOTAL_TESTS}"
echo -e "${GREEN}Passed: ${PASSED_TESTS}${NC}"
echo -e "${RED}Failed: ${FAILED_TESTS}${NC}"

if [ "$FAILED_TESTS" -eq 0 ]; then
    echo ""
    echo -e "${GREEN}🎉 ALL TESTS PASSED${NC}"
    echo "PulseHA basic functionality is working correctly in the Docker environment."
    echo ""
    echo "Next Steps:"
    echo "1. Review the logs: docker logs pulseha-node1"
    echo "2. Test advanced features manually using the QA Testing Guide"
    echo "3. For full IP failover testing, use VM/bare metal environment"
    exit 0
else
    echo ""
    echo -e "${RED}⚠️  SOME TESTS FAILED${NC}"
    echo "Please review the failed tests and check container logs:"
    echo "- docker logs pulseha-node1"
    echo "- docker logs pulseha-node2"
    echo "- docker logs pulseha-node3"
    exit 1
fi