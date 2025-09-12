//go:build !linux

package membership

// monitorLoop is a no-op on non-Linux platforms
func (m *IPMonitor) monitorLoop() {}

