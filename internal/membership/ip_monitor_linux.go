//go:build linux

package membership

import (
	"strings"
	"time"

	"github.com/syleron/pulseha/packages/network"
	"github.com/syleron/pulseha/packages/utils"
	"github.com/vishvananda/netlink"
)

// startMonitorLoop uses netlink address subscription for event-driven reconciliation
func (m *IPMonitor) monitorLoop() {
	updates := make(chan netlink.AddrUpdate, 32)
	if err := netlink.AddrSubscribe(updates, m.stopChan); err != nil {
		m.logger.Error("IP monitor: failed to subscribe to netlink addr updates", "error", err)
		return
	}

	m.logger.Info("IP monitor: netlink address subscription active")

	for {
		select {
		case <-m.stopChan:
			return
		case upd, ok := <-updates:
			if !ok {
				return
			}

			link, err := netlink.LinkByIndex(upd.LinkIndex)
			if err != nil || link == nil {
				continue
			}
			iface := link.Attrs().Name

			// Snapshot expected IPs for this interface
			m.RLock()
			expected := make([]string, len(m.expectedIPs[iface]))
			copy(expected, m.expectedIPs[iface])
			// Also capture expected set across all interfaces to resolve correct iface
			allExpected := make(map[string]string) // ip(without mask)->iface
			for ifn, ips := range m.expectedIPs {
				for _, e := range ips {
					ipOnly := e
					if strings.Contains(e, "/") {
						ipOnly = strings.Split(e, "/")[0]
					}
					allExpected[ipOnly] = ifn
				}
			}
			m.RUnlock()

			changedIP := upd.LinkAddress.IP.String()
			// Construct netlink.Addr from update
			addrStr := upd.LinkAddress.String()
			addrObj, perr := netlink.ParseAddr(addrStr)
			if perr != nil {
				continue
			}

			// Evaluate local role
			localID, err := m.members.config.GetLocalNodeUUID()
			if err != nil {
				continue
			}
			localMember := m.members.GetMemberByID(localID)
			if localMember == nil {
				continue
			}

			if upd.NewAddr {
				// Address added
				if localMember.Status != StatusActive {
					// Drop any VIP additions while passive
					_ = netlink.AddrDel(link, addrObj)
					m.logger.Info("IP monitor: dropped IP on passive node", "ip", changedIP, "iface", iface)
					continue
				}
				// Active: allow only expected IPs on the correct interface
				correctIface, isExpected := allExpected[changedIP]
				if !isExpected {
					// Unexpected addition; remove
					_ = netlink.AddrDel(link, addrObj)
					m.logger.Warn("IP monitor: removed unexpected IP on active node", "ip", changedIP, "iface", iface)
					continue
				}
				if correctIface != iface {
					// Move to correct interface
					_ = netlink.AddrDel(link, addrObj)
					if targetLink, e := netlink.LinkByName(correctIface); e == nil {
						_ = netlink.AddrAdd(targetLink, addrObj)
					}
					m.logger.Info("IP monitor: moved IP to correct interface", "ip", changedIP, "from", iface, "to", correctIface)
				}
				continue
			}

			// Address removed: restore expected IPs ONLY if we're currently Active
			if len(expected) == 0 {
				m.logger.Debug("IP monitor: no expected IPs for removed address", "removedIP", changedIP, "iface", iface)
				continue
			}

			// Re-check current node status before attempting restore
			localID, err2 := m.members.config.GetLocalNodeUUID()
			if err2 != nil {
				m.logger.Debug("IP monitor: failed to get local node ID for restore check", "error", err2)
				continue
			}
			currentMember := m.members.GetMemberByID(localID)
			if currentMember == nil {
				m.logger.Debug("IP monitor: local member not found for restore check")
				continue
			}

			if currentMember.Status != StatusActive {
				m.logger.Info("IP monitor: IP removed but node is not Active, NOT restoring", "ip", changedIP, "status", currentMember.Status, "iface", iface)
				continue
			}

			// If an expected IP was removed and we're Active, immediately restore
			for _, exp := range expected {
				ipOnly := exp
				if strings.Contains(exp, "/") {
					ipOnly = strings.Split(exp, "/")[0]
				}
				if ipOnly == changedIP {
					m.logger.Warn("IP monitor: expected IP removed from Active node; restoring", "ip", exp, "iface", iface, "status", currentMember.Status)
					m.restoreIP(iface, exp)
					break
				}
			}
		}
	}
}

// restoreIP attempts to restore an IP that was unexpectedly removed on Linux
func (m *IPMonitor) restoreIP(iface string, ip string) {
	m.logger.Debug("IP monitor restore: starting restore", "iface", iface, "ip", ip)

	link, err := netlink.LinkByName(iface)
	if err != nil {
		m.logger.Error("IP monitor restore: failed to get link", "iface", iface, "error", err)
		return
	}
	m.logger.Debug("IP monitor restore: got netlink interface", "iface", iface)

	// Determine CIDR if missing
	cidr := ip
	if !strings.Contains(ip, "/") {
		if strings.Contains(ip, ":") {
			cidr = ip + "/128"
		} else {
			cidr = ip + "/32"
		}
		m.logger.Debug("IP monitor restore: added CIDR notation", "originalIP", ip, "cidr", cidr)
	}

	addr, err := netlink.ParseAddr(cidr)
	if err != nil {
		m.logger.Error("IP monitor restore: failed to parse addr", "cidr", cidr, "error", err)
		return
	}
	m.logger.Debug("IP monitor restore: parsed address", "cidr", cidr)

	if err := netlink.AddrAdd(link, addr); err != nil {
		m.logger.Error("IP monitor restore: failed to add addr", "cidr", cidr, "iface", iface, "error", err)
		return
	}
	m.logger.Info("IP monitor restore: successfully restored expected IP", "iface", iface, "ip", ip)
}

// periodicReconcile runs a lightweight reconcile loop to enforce expected IPs
func (m *IPMonitor) periodicReconcile() {
	// Run every 30s; exit on stop
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.stopChan:
			return
		case <-t.C:
			m.enforceExpectations()
		}
	}
}

// enforceExpectations ensures that the current local role and expectedIPs are reflected on interfaces
func (m *IPMonitor) enforceExpectations() {
	// Determine local role
	localID, err := m.members.config.GetLocalNodeUUID()
	if err != nil {
		return
	}
	member := m.members.GetMemberByID(localID)
	if member == nil {
		return
	}

	m.RLock()
	expectations := make(map[string][]string, len(m.expectedIPs))
	for iface, ips := range m.expectedIPs {
		cpy := make([]string, len(ips))
		copy(cpy, ips)
		expectations[iface] = cpy
	}
	m.RUnlock()

	// Passive: remove all floating IPs; Active: ensure missing are added
	if member.Status != StatusActive {
		// Remove any floating IPs that shouldn't be on passive nodes
		for iface, ips := range expectations {
			for _, ip := range ips {
				ipOnly, _ := utils.GetCIDR(ip)
				if ipOnly == nil {
					continue
				}
				exists, foundIface, _ := network.CheckIfIPExists(ipOnly.String())
				if exists && foundIface == iface {
					m.logger.Info("IP monitor: removing floating IP from passive node", "ip", ip, "iface", iface)
					if err := network.BringIPdown(iface, ip); err != nil {
						m.logger.Warn("IP monitor: failed to remove floating IP from passive node", "ip", ip, "iface", iface, "error", err)
					}
				}
			}
		}
		return
	}

	// Bring up any missing IPs per interface
	for iface, ips := range expectations {
		var missing []string
		for _, ip := range ips {
			ipOnly, _ := utils.GetCIDR(ip)
			if ipOnly == nil {
				continue
			}
			exists, eIface, _ := network.CheckIfIPExists(ipOnly.String())
			if !exists || eIface != iface {
				missing = append(missing, ip)
			}
		}
		if len(missing) > 0 {
			for _, ip := range missing {
				_ = network.BringIPup(iface, ip)
			}
		}
	}
}
