package membership

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vishvananda/netlink"
)

// IPMonitor monitors IP addresses on interfaces and ensures they match the expected configuration
type IPMonitor struct {
	sync.RWMutex
	members        *MemberList
	logger         *logrus.Logger
	expectedIPs    map[string][]string // map[interface][]ips
	stopChan       chan struct{}
	stopOnce       sync.Once
	done           chan struct{}
}

// NewIPMonitor creates a new IP monitor
func NewIPMonitor(members *MemberList, logger *logrus.Logger) *IPMonitor {
	return &IPMonitor{
		members:     members,
		logger:      logger,
		expectedIPs: make(map[string][]string),
		stopChan:    make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// Start begins monitoring IP addresses
func (m *IPMonitor) Start() error {
	m.Lock()
	defer m.Unlock()

	// Initialize the expected IPs from the current member
	if err := m.initializeExpectedIPs(); err != nil {
		return fmt.Errorf("failed to initialize expected IPs: %v", err)
	}

	// Start periodic verification
	go m.periodicVerification()

	m.logger.Info("IP monitor started")
	return nil
}

// Stop stops the IP monitor
func (m *IPMonitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Info("IP monitor stopped")
	})
}

// UpdateExpectedIPs updates the list of expected IPs for an interface
func (m *IPMonitor) UpdateExpectedIPs(iface string, ips []string) {
	m.Lock()
	defer m.Unlock()

	// Create a copy of the IPs slice to avoid external modifications
	ipsCopy := make([]string, len(ips))
	copy(ipsCopy, ips)

	m.expectedIPs[iface] = ipsCopy
	m.logger.Infof("Updated expected IPs for interface %s: %v", iface, ips)
}

// RemoveExpectedIPs removes IPs from the expected list for an interface
func (m *IPMonitor) RemoveExpectedIPs(iface string, ips []string) {
	m.Lock()
	defer m.Unlock()

	// Create a map for quick lookup
	toRemove := make(map[string]bool)
	for _, ip := range ips {
		toRemove[ip] = true
	}

	// Filter out the IPs to remove
	current := m.expectedIPs[iface]
	var updated []string
	for _, ip := range current {
		if !toRemove[ip] {
			updated = append(updated, ip)
		}
	}

	m.expectedIPs[iface] = updated
	m.logger.Infof("Removed IPs from interface %s, remaining: %v", iface, updated)
}

// ClearExpectedIPs removes all expected IPs for an interface
func (m *IPMonitor) ClearExpectedIPs(iface string) {
	m.Lock()
	defer m.Unlock()

	delete(m.expectedIPs, iface)
	m.logger.Infof("Cleared all expected IPs for interface %s", iface)
}

// initializeExpectedIPs initializes the expected IPs from the current member
func (m *IPMonitor) initializeExpectedIPs() error {
	// Get the local member
	localNodeID, err := m.members.config.GetLocalNodeUUID()
	if err != nil {
		return fmt.Errorf("failed to get local node ID: %v", err)
	}

	localMember := m.members.GetMemberByID(localNodeID)
	if localMember == nil {
		return fmt.Errorf("local member not found")
	}

	// Get the active IPs for the local member
	localMember.Lock()
	activeIPs := make([]string, len(localMember.ActiveIPs))
	copy(activeIPs, localMember.ActiveIPs)
	localMember.Unlock()

	// Group IPs by interface
	for _, ip := range activeIPs {
		// Get the interface for this IP
		iface, err := m.members.config.GetGroupIface(localMember.Hostname, "")
		if err != nil {
			m.logger.Warnf("Failed to get interface for IP %s: %v", ip, err)
			continue
		}

		// Add to expected IPs
		m.expectedIPs[iface] = append(m.expectedIPs[iface], ip)
	}

	return nil
}

// monitorLoop processes netlink updates (disabled for now due to API changes)
func (m *IPMonitor) monitorLoop() {
	// NetLink monitoring disabled due to API changes
	// This functionality will be restored in a future version
	return
}

// restoreIP attempts to restore an IP that was unexpectedly removed
func (m *IPMonitor) restoreIP(iface string, ip string) {
	m.logger.Infof("Attempting to restore IP %s on interface %s", ip, iface)

	// Get the link
	link, err := netlink.LinkByName(iface)
	if err != nil {
		m.logger.Errorf("Failed to get link for interface %s: %v", iface, err)
		return
	}

	// Parse the IP
	addr, err := netlink.ParseAddr(ip + "/32") // Assuming /32 for simplicity
	if err != nil {
		m.logger.Errorf("Failed to parse IP %s: %v", ip, err)
		return
	}

	// Add the IP to the interface
	if err := netlink.AddrAdd(link, addr); err != nil {
		m.logger.Errorf("Failed to add IP %s to interface %s: %v", ip, iface, err)
		return
	}

	m.logger.Infof("Successfully restored IP %s on interface %s", ip, iface)
}

// periodicVerification periodically verifies that all expected IPs are present
func (m *IPMonitor) periodicVerification() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.verifyAllIPs()
		}
	}
}

// verifyAllIPs verifies that all expected IPs are present on their interfaces
func (m *IPMonitor) verifyAllIPs() {
	m.RLock()
	// Create a copy to avoid holding the lock during potentially slow operations
	expectedCopy := make(map[string][]string)
	for iface, ips := range m.expectedIPs {
		ipsCopy := make([]string, len(ips))
		copy(ipsCopy, ips)
		expectedCopy[iface] = ipsCopy
	}
	m.RUnlock()

	for iface, expectedIPs := range expectedCopy {
		// Get the actual IPs on this interface
		actualIPs, err := m.getInterfaceIPs(iface)
		if err != nil {
			m.logger.Errorf("Failed to get IPs for interface %s: %v", iface, err)
			continue
		}

		// Check for missing IPs
		for _, expectedIP := range expectedIPs {
			found := false
			for _, actualIP := range actualIPs {
				if actualIP == expectedIP {
					found = true
					break
				}
			}

			if !found {
				m.logger.Warnf("Expected IP %s not found on interface %s", expectedIP, iface)
				// Restore the IP
				m.restoreIP(iface, expectedIP)
			}
		}
	}
}

// getInterfaceIPs gets all IPs assigned to an interface
func (m *IPMonitor) getInterfaceIPs(iface string) ([]string, error) {
	// Get the interface
	intf, err := net.InterfaceByName(iface)
	if err != nil {
		return nil, fmt.Errorf("interface not found: %v", err)
	}

	// Get addresses
	addrs, err := intf.Addrs()
	if err != nil {
		return nil, fmt.Errorf("failed to get addresses: %v", err)
	}

	// Extract IPs
	var ips []string
	for _, addr := range addrs {
		// Parse the address
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ips = append(ips, ipNet.IP.String())
	}

	return ips, nil
}
