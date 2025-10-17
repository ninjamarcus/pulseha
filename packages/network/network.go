// PulseHA - HA Cluster Daemon
// Copyright (C) 2017-2021  Andrew Zak <andrew@linux.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package network

import (
	"bytes"
	"errors"
	"net"
	"os/exec"
	"strings"

	log "github.com/charmbracelet/log"
	"github.com/syleron/pulseha/packages/utils"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

type ICMPv6MessageHeader struct {
	Type     byte
	Code     byte
	Checksum uint16
}

type ICMPv6NeighborSolicitation struct {
	Header            ICMPv6MessageHeader
	Reserved          uint32
	TargetAddress     [16]byte
	OptionType        byte
	OptionLength      byte
	SourceLinkAddress [6]byte
}

/*
*
Send Gratuitous ARP to automagically tell the router who has the new floating IP
NOTE: This function assumes the OS is LINUX and has "arping" installed.
*/
func SendGARP(iface, ip string) error {
	exists, _ := InterfaceExist(iface)
	if !exists {
		log.Error("Unable to GARP as the network interface does not exist")
		return errors.New("network interface does not exist")
	}
	var garpIP net.IP
	if parsedIP := net.ParseIP(ip); parsedIP != nil {
		garpIP = parsedIP
	} else {
		parsedIP, _, err := net.ParseCIDR(ip)
		if err != nil {
			log.Error("failed to GARP. Cannot parse IP address", "value", ip, "error", err)
			return err
		}
		garpIP = parsedIP
	}
	log.Debug("Sending gratuitous arp for " + garpIP.String() + " on interface " + iface)
	_, err := utils.Execute("arping", "-U", "-c", "5", "-I", iface, garpIP.String())
	if err != nil {
		log.Error("failed to GARP. " + err.Error())
		return err
	}
	return nil
}

/*
*
Checks to see what status a network interface is currently.
Possible responses are either up or down.
*/
func netInterfaceStatus(iface string) bool {
	link, err := netlink.LinkByName(iface)
	if err != nil {
		log.Debug("netInterfaceStatus: unable to resolve interface", "iface", iface, "error", err)
		return false
	}
	attrs := link.Attrs()
	if attrs == nil {
		return false
	}
	return attrs.OperState == netlink.OperUp
}

/*
*
This function is to bring up a network interface
*/
func BringIPup(iface, ip string) error {
	log.Info("NETWORK: Starting BringIPup", "iface", iface, "ip", ip)
	exists, link := InterfaceExist(iface)
	if !exists {
		log.Error("NETWORK: Interface does not exist", "iface", iface)
		return errors.New("unable to bring IP up as the network interface does not exist")
	}
	log.Debug("NETWORK: Interface exists", "iface", iface)

	// Check to see if the ip already exists
	ipOb, ipNet := utils.GetCIDR(ip)
	log.Debug("NETWORK: GetCIDR result", "inputIP", ip, "ipOnly", ipOb, "ipNet", ipNet)
	if ipOb == nil {
		log.Error("NETWORK: GetCIDR returned nil IP for input", "ip", ip)
		return errors.New("invalid IP address format")
	}

	exists, eIface, err := CheckIfIPExists(ipOb.String())
	if err != nil {
		log.Debug("NETWORK: Failed to check if IP exists", "error", err)
		return err
	}
	log.Info("NETWORK: IP existence check", "ip", ipOb.String(), "exists", exists, "existingIface", eIface, "targetIface", iface)

	if exists {
		// If IP already exists on the target interface, we're done
		if eIface == iface {
			log.Info("NETWORK: IP already exists on target interface (nothing to do)", "ip", ipOb.String(), "iface", iface)
			return nil
		}
		// If IP exists on another interface, bring it down first
		log.Info("NETWORK: IP exists on different interface, removing first", "ip", ipOb.String(), "currentIface", eIface, "targetIface", iface)
		if err := BringIPdown(eIface, ip); err != nil {
			log.Warn("NETWORK: Failed to remove IP from existing interface", "ip", ipOb.String(), "iface", eIface, "error", err)
		} else {
			log.Info("NETWORK: Successfully removed IP from existing interface", "ip", ipOb.String(), "iface", eIface)
		}
	}

	addr, err := netlink.ParseAddr(ip)
	if err != nil {
		log.Error("NETWORK: Failed to parse address", "ip", ip, "error", err)
		return errors.New("unable to bring IP up because ip address couldn't be parsed")
	}
	log.Debug("NETWORK: Parsed address successfully", "ip", ip)

	log.Info("NETWORK: Adding IP to interface", "ip", ip, "iface", iface)
	if err := netlink.AddrAdd(link, addr); err != nil {
		log.Error("NETWORK: netlink.AddrAdd failed", "error", err, "ip", ip, "iface", iface)
		return errors.New("unable to bring IP up as netlink failed to do so")
	}
	log.Info("NETWORK: Successfully brought up IP", "ip", ip, "iface", iface)
	return nil
}

/*
*
This function is to bring down a network interface
*/
func BringIPdown(iface, ip string) error {
	log.Debug("Attempting to bring down IP address via network package")
	exists, link := InterfaceExist(iface)
	if !exists {
		log.Debug("unable to bring IP down as the network interface does not exist")
		return errors.New("unable to bring IP down as the network interface does not exist")
	}
	addr, err := netlink.ParseAddr(ip)
	if err != nil {
		log.Debug("unable to bring IP down because ip address couldn't be parsed")
		return errors.New("unable to bring IP down because ip address couldn't be parsed")
	}
	if err := netlink.AddrDel(link, addr); err != nil {
		log.Debug("Unable to bring down ip " + ip + " on interface " + iface + ". Perhaps it doesn't exist?")
		return errors.New("Unable to bring down ip " + ip + " on interface " + iface + ". Perhaps it doesn't exist?")
	}
	return nil
}

/*
*
Perform a curl request to a web host.
This only returns a boolean based off the http status code received by the request.
*/
func Curl(httpRequestURL string) bool {
	output, err := utils.Execute("curl", "-s", "-o", "/dev/null", "-w", "\"%{http_code}\"", httpRequestURL)
	if err != nil {
		//log.Error("Http Curl request failed.")
		return false
	}
	if output == "\"200\"" {
		return true
	} else {
		return false
	}
}

/**
 * Performs an ICMP ping to check if a host is reachable
 * Handles both plain IPs and CIDR notation
 */
func ICMPv4(Ipv4Addr string) error {
	// If the IP is in CIDR notation, extract just the IP part
	if strings.Contains(Ipv4Addr, "/") {
		ipPart, _, err := net.ParseCIDR(Ipv4Addr)
		if err != nil {
			log.Error("Failed to parse CIDR address: ", Ipv4Addr)
			return err
		}
		Ipv4Addr = ipPart.String()
	}

	cmds := "ping -c 1 -W 1 " + Ipv4Addr + " &> /dev/null ; echo $?"
	cmd := exec.Command("bash", "-c", cmds)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Error("ICMP request failed: ", Ipv4Addr)
		return err
	}
	if !strings.Contains(out.String(), "0") {
		log.Error("ICMP request failed: ", Ipv4Addr, " ", out.String())
		return errors.New("failed to reach host")
	}
	return nil
}

/*
*
Function to perform an arp scan on the network. This will allow us to see which IP's are available.
*/
func ArpScan(addrWSubnet string) string {
	output, err := utils.Execute("arp-scan", addrWSubnet)
	if err != nil {
		return err.Error()
	}
	return output
}

/*
*
Send the eq. of IPv4 arping with IPv6
*/
func IPv6NDP(ipv6Iface string) string {
	output, err := utils.Execute("ndptool", "-t", "na", "-U", "-i", ipv6Iface)
	if err != nil {
		return err.Error()
	}
	return output
}

/*
*
Return network interface names
*/
func GetInterfaceNames() []string {
	log.Debug("Network Package - GetInerfacesNames()")
	links, err := netlink.LinkList()
	if err != nil {
		log.Debug("Network Package - GetInterfaceNames() Error retrieving network links via netlink. ", err)
		return nil
	}
	var interfaceNames []string
	for _, link := range links {
		attrs := link.Attrs()
		if attrs != nil && attrs.Slave == nil {
			interfaceNames = append(interfaceNames, attrs.Name)
		}
	}
	return interfaceNames
}

/*
*
Check if an interface exists on the local node
*/
func InterfaceExist(name string) (bool, netlink.Link) {
	log.Debug("Network Package - InterfaceExists()")
	link, err := netlink.LinkByName(name)
	if err != nil {
		log.Debug(err)
		return false, nil
	}
	return true, link
}

/*
*
Checks to see if an IP exists on an interface already
*/
func CheckIfIPExists(ipAddr string) (bool, string, error) {
	log.Debug("CheckIfIPExists called", "searchIP", ipAddr)

	// Parse the input as either IP or CIDR and extract the IP portion only
	var targetIP net.IP
	if strings.Contains(ipAddr, "/") {
		parsedIP, _, err := net.ParseCIDR(ipAddr)
		if err != nil {
			log.Debug("CheckIfIPExists invalid CIDR", "input", ipAddr, "error", err)
			return false, "", err
		}
		targetIP = parsedIP
	} else {
		targetIP = net.ParseIP(ipAddr)
		if targetIP == nil {
			log.Debug("CheckIfIPExists invalid IP", "input", ipAddr)
			return false, "", errors.New("invalid IP address: " + ipAddr)
		}
	}

	// Determine address family (IPv4 or IPv6)
	family := unix.AF_INET
	isV4 := targetIP.To4() != nil
	if !isV4 {
		// Normalize to 16-byte form for IPv6 comparisons
		targetIP = targetIP.To16()
		if targetIP == nil {
			log.Debug("CheckIfIPExists unsupported IP family", "input", ipAddr)
			return false, "", errors.New("unsupported IP address family for: " + ipAddr)
		}
		family = unix.AF_INET6
	} else {
		// Normalize IPv4 to 4-byte form
		targetIP = targetIP.To4()
	}
	targetStr := targetIP.String()

	links, err := netlink.LinkList()
	if err != nil {
		log.Debug("Network Package - CheckIfIPExists() Failed to get network links via netlink. ", err)
		return false, "", err
	}

	for _, link := range links {
		// Get IP addresses for link for the determined family
		addrs, err := netlink.AddrList(link, family)
		if err != nil {
			log.Debug("Network Package - CheckIfIPExists() Failed to get addresses for link via netlink. ", err)
			return false, "", err
		}
		for _, addr := range addrs {
			addrIP := addr.IP
			if addrIP == nil {
				continue
			}
			if isV4 {
				addrIP = addrIP.To4()
				if addrIP == nil {
					continue
				}
			} else {
				addrIP = addrIP.To16()
				if addrIP == nil || addrIP.To4() != nil { // skip v4-mapped
					continue
				}
			}
			log.Debug("CheckIfIPExists comparing", "searchIP", targetStr, "foundIP", addrIP.String(), "interface", link.Attrs().Name)
			if targetIP.Equal(addrIP) {
				log.Debug("CheckIfIPExists found match", "ip", targetStr, "interface", link.Attrs().Name)
				return true, link.Attrs().Name, nil
			}
		}
	}

	log.Debug("CheckIfIPExists not found", "ip", targetStr)
	return false, "", nil
}
