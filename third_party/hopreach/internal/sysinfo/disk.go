// UK Mesh modifications, 2026-08-02. Licensed with HopReach under
// AGPL-3.0 plus Commons Clause.
package sysinfo

import (
	"fmt"
	"syscall"
)

// FreeDiskBytes reports unprivileged-user available bytes for the filesystem
// containing path. HopReach uses this immediately before Precision starts.
func FreeDiskBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
