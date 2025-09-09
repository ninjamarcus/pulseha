//go:build darwin
// +build darwin

package uptime

import (
    "regexp"
    "strconv"
    "time"

    "golang.org/x/sys/unix"
)

// Get returns the system uptime on macOS using sysctl kern.boottime
func Get() (time.Duration, error) {
    // Example output: "{ sec = 1694111385, usec = 0 } Mon Sep  7 12:29:45 2025"
    s, err := unix.Sysctl("kern.boottime")
    if err != nil {
        return 0, err
    }
    re := regexp.MustCompile(`sec\s*=\s*([0-9]+)`) 
    m := re.FindStringSubmatch(s)
    if len(m) < 2 {
        return 0, nil
    }
    sec, err := strconv.ParseInt(m[1], 10, 64)
    if err != nil {
        return 0, err
    }
    boot := time.Unix(sec, 0)
    return time.Since(boot), nil
}


