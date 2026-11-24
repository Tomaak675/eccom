//go:build windows

package environment

// openFilesLimit has no equivalent on Windows (no RLIMIT_NOFILE).
func openFilesLimit() (any, any) {
	return nil, nil
}
