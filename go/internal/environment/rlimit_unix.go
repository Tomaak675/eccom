//go:build unix

package environment

import "syscall"

// openFilesLimit returns the soft and hard RLIMIT_NOFILE of this process.
func openFilesLimit() (any, any) {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return nil, nil
	}
	return int64(limit.Cur), int64(limit.Max)
}
