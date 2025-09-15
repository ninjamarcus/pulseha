//go:build linux

package membership

import (
	"strings"

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

			// Address removed: restore expected IPs
			if len(expected) == 0 {
				continue
			}

			// If an expected IP was removed, immediately restore
			for _, exp := range expected {
				ipOnly := exp
				if strings.Contains(exp, "/") {
					ipOnly = strings.Split(exp, "/")[0]
				}
				if ipOnly == changedIP {
					m.logger.Warn("IP monitor: expected IP removed; restoring", "ip", exp, "iface", iface)
					m.restoreIP(iface, exp)
					break
				}
			}
		}
	}
}

// restoreIP attempts to restore an IP that was unexpectedly removed on Linux
func (m *IPMonitor) restoreIP(iface string, ip string) {
	link, err := netlink.LinkByName(iface)
	if err != nil {
		m.logger.Error("IP monitor: failed to get link", "iface", iface, "error", err)
		return
	}
	// Determine CIDR if missing
	cidr := ip
	if !strings.Contains(ip, "/") {
		if strings.Contains(ip, ":") {
			cidr = ip + "/128"
		} else {
			cidr = ip + "/32"
		}
	}
	addr, err := netlink.ParseAddr(cidr)
	if err != nil {
		m.logger.Error("IP monitor: failed to parse addr", "cidr", cidr, "error", err)
		return
	}
	if err := netlink.AddrAdd(link, addr); err != nil {
		m.logger.Error("IP monitor: failed to add addr", "cidr", cidr, "error", err)
		return
	}
	m.logger.Info("IP monitor: restored expected IP", "iface", iface, "ip", ip)
}
