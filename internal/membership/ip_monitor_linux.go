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
			m.RUnlock()
			if len(expected) == 0 {
				continue
			}

			// Changed address without mask -> string
			changedIP := upd.LinkAddress.IP.String()

			// If an expected IP was removed, immediately restore
			if !upd.NewAddr {
				for _, exp := range expected {
					if exp == changedIP {
						m.logger.Warn("IP monitor: expected IP removed; restoring", "ip", exp, "iface", iface)
						m.restoreIP(iface, exp)
						break
					}
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
