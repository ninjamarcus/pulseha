package services

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/syleron/pulseha/packages/utils"
	"github.com/vishvananda/netlink"
	"net"
)

func SendGARP(iface, ip string) bool {
	// Check if the interface exists
	exists, err := InterfaceExist(iface)
	if err != nil {
		log.Errorf("Error checking interface existence: %v", err)
		return false
	}
	if !exists {
		log.Errorf("Network interface %s does not exist. Aborting GARP.", iface)
		return false
	}

	// Parse the IP to CIDR
	cidrIP, _, err := net.ParseCIDR(ip)
	if err != nil {
		log.Errorf("Failed to parse CIDR for IP %s: %v", ip, err)
		return false
	}

	log.Debugf("Sending gratuitous ARP for %s on interface %s", cidrIP.String(), iface)

	// Execute the ARP command
	_, err = utils.Execute("arping", "-U", "-c", "5", "-I", iface, cidrIP.String())
	if err != nil {
		log.Errorf("Failed to execute GARP: %v", err)
		return false
	}

	log.Infof("Successfully sent gratuitous ARP for %s on %s", cidrIP.String(), iface)
	return true
}

func BringIPup(iface, ip string) error {
	log.Debug("Attempting to bring up IP address via network package")

	// Verify if the interface exists
	exists, link := InterfaceExist(iface)
	if !exists {
		return errors.New("network interface does not exist: unable to bring IP up")
	}

	// Parse IP as CIDR format
	ipOb, err := utils.GetCIDR(ip)
	if err != nil {
		log.Debug("Failed to parse IP as CIDR: ", err)
		return errors.New("invalid IP format")
	}

	// Check if IP already exists on any interface
	ipExists, eIface, err := CheckIfIPExists(ipOb.String())
	if err != nil {
		log.Debug("Failed to check IP existence: ", err)
		return err
	}

	// If IP exists, attempt to bring it down first
	if ipExists {
		if err := BringIPdown(eIface, ip); err != nil {
			log.Debugf("IP %s was up on %s, attempting to bring it down failed: %v", ipOb.String(), eIface, err)
		}
	}

	// Parse IP for netlink
	addr, err := netlink.ParseAddr(ip)
	if err != nil {
		log.Debug("Failed to parse IP address for netlink: ", err)
		return errors.New("unable to bring IP up: IP address parsing failed")
	}

	// Add IP address to the interface using netlink
	if err := netlink.AddrAdd(link, addr); err != nil {
		log.Debug("Netlink failed to add IP address: ", err)
		return errors.New("unable to bring IP up: netlink operation failed")
	}

	log.Infof("Successfully brought up IP %s on interface %s", ipOb.String(), iface)
	return nil
}

func BringIPdown(iface, ip string) error {
	log.Debug("Attempting to bring down IP address via network package")

	// Check if the network interface exists
	exists, link, err := InterfaceExist(iface)
	if !exists {
		errMsg := "Network interface does not exist: unable to bring IP down"
		log.Debug(errMsg)
		return errors.New(errMsg)
	}

	// Parse the IP address
	addr, err := netlink.ParseAddr(ip)
	if err != nil {
		errMsg := "Failed to parse IP address: unable to bring IP down"
		log.Debug(errMsg)
		return errors.New(errMsg)
	}

	// Attempt to remove the IP address from the interface
	if err := netlink.AddrDel(link, addr); err != nil {
		errMsg := "Unable to bring down IP " + ip + " on interface " + iface + ": possibly already down"
		log.Debug(errMsg)
		return errors.New(errMsg)
	}

	log.Infof("Successfully brought down IP %s on interface %s", ip, iface)
	return nil
}

func IPv6NDP(ipv6Iface string) (string, error) {
	// Execute the ndptool command
	output, err := utils.Execute("ndptool", "-t", "na", "-U", "-i", ipv6Iface)
	if err != nil {
		return "", fmt.Errorf("failed to send IPv6 NDP on interface %s: %v", ipv6Iface, err)
	}

	return output, nil
}

func GetInterfaceNames() ([]string, error) {
	log.Debug("Fetching network interface names")

	// Retrieve the list of network interfaces
	links, err := netlink.LinkList()
	if err != nil {
		log.Debug("Error retrieving network links: ", err)
		return nil, fmt.Errorf("failed to retrieve network interfaces: %v", err)
	}

	var interfaceNames []string
	for _, iface := range links {
		// Add only non-slave interfaces
		if iface.Attrs().Slave == nil {
			interfaceNames = append(interfaceNames, iface.Attrs().Name)
		}
	}

	return interfaceNames, nil
}

func InterfaceExist(name string) (bool, netlink.Link, error) {
	log.Debug("Checking if network interface exists:", name)

	link, err := netlink.LinkByName(name)
	if err != nil {
		log.Debugf("Interface %s not found: %v", name, err)
		return false, nil, fmt.Errorf("interface %s does not exist: %v", name, err)
	}

	return true, link, nil
}

func CheckIfIPExists(ipAddr string) (bool, string, error) {
	log.Debug("Checking if IP address exists:", ipAddr)

	links, err := netlink.LinkList()
	if err != nil {
		log.Debugf("Failed to retrieve network links: %v", err)
		return false, "", fmt.Errorf("error retrieving network links: %v", err)
	}

	for _, link := range links {
		// Retrieve all IPv4 addresses for the link
		addrs, err := netlink.AddrList(link, 4)
		if err != nil {
			log.Debugf("Failed to retrieve addresses for link %s: %v", link.Attrs().Name, err)
			return false, "", fmt.Errorf("error retrieving addresses for link %s: %v", link.Attrs().Name, err)
		}

		// Check each address to see if it matches the given IP
		for _, addr := range addrs {
			if ipAddr == addr.IP.String() {
				return true, addr.Label, nil
			}
		}
	}

	return false, "", nil
}
