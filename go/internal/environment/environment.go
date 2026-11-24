// Package environment writes <host>-environment.json with the same keys as
// eccom/instrumentation/environment.py, so runs from either agent document
// themselves alike, plus the Go toolchain and the versions of its modules.
package environment

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
)

var trackedModules = []string{
	"github.com/eclipse/paho.golang",
	"github.com/shirou/gopsutil/v4",
	"gopkg.in/yaml.v3",
}

// Collect gathers the environment record for host role hostRole.
func Collect(hostRole string) map[string]any {
	hostname, _ := os.Hostname()
	info, _ := host.Info()
	var kernelVersion, kernelArch, platform any
	if info != nil {
		kernelVersion, kernelArch = info.KernelVersion, info.KernelArch
		platform = strings.Join([]string{info.OS, info.KernelVersion, info.KernelArch}, "-")
	}
	var cpuModel any
	if infos, err := cpu.Info(); err == nil && len(infos) > 0 && infos[0].ModelName != "" {
		cpuModel = infos[0].ModelName
	}
	soft, hard := openFilesLimit()
	return map[string]any{
		"host_role":      hostRole,
		"implementation": "go",
		"hostname":       hostname,
		"collected_at":   time.Now().UTC().Format(time.RFC3339Nano),
		"python_version": nil,
		"go_version":     runtime.Version(),
		"go_max_procs":   runtime.GOMAXPROCS(0),
		"platform":       platform,
		"os":             strings.ToUpper(runtime.GOOS[:1]) + runtime.GOOS[1:],
		"kernel_version": kernelVersion,
		"cpu": map[string]any{
			"model":                 cpuModel,
			"architecture":          kernelArch,
			"logical_cores":         runtime.NumCPU(),
			"container_limit_cores": containerCPULimit("/sys/fs/cgroup"),
		},
		"open_files_limit":    map[string]any{"soft": soft, "hard": hard},
		"git_commit":          gitCommit(),
		"docker_image_digest": envOrNil("ECCOM_IMAGE_DIGEST"),
		"dependencies":        moduleVersions(),
		"clock_sync": map[string]any{
			"method":       nil,
			"synchronized": nil,
			"note":         "no clock synchronization information collected",
		},
	}
}

// Write stores Collect(hostRole) as <runDir>/<hostRole>-environment.json.
// Returns the first file or encoding error.
func Write(hostRole, runDir string) error {
	data, err := json.MarshalIndent(Collect(hostRole), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(runDir, hostRole+"-environment.json"), data, 0o644)
}

// containerCPULimit is the cores allowed by the cgroup's CFS quota (cgroup
// v2 cpu.max or v1 cpu.cfs_quota_us / cpu.cfs_period_us) under root, the
// cgroup mount as the container sees it; nil when unlimited or unreadable.
func containerCPULimit(root string) any {
	if data, err := os.ReadFile(filepath.Join(root, "cpu.max")); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) == 2 {
			if fields[0] == "max" {
				return nil
			}
			quota, errQ := strconv.ParseFloat(fields[0], 64)
			period, errP := strconv.ParseFloat(fields[1], 64)
			if errQ == nil && errP == nil && period > 0 {
				return quota / period
			}
		}
	}
	for _, subdir := range []string{"cpu", "cpu,cpuacct"} {
		quotaData, errQ := os.ReadFile(filepath.Join(root, subdir, "cpu.cfs_quota_us"))
		periodData, errP := os.ReadFile(filepath.Join(root, subdir, "cpu.cfs_period_us"))
		if errQ != nil || errP != nil {
			continue
		}
		quota, errQ := strconv.ParseFloat(strings.TrimSpace(string(quotaData)), 64)
		period, errP := strconv.ParseFloat(strings.TrimSpace(string(periodData)), 64)
		if errQ != nil || errP != nil || period <= 0 {
			continue
		}
		if quota < 0 {
			return nil
		}
		return quota / period
	}
	return nil
}

func moduleVersions() map[string]any {
	versions := make(map[string]any, len(trackedModules))
	for _, path := range trackedModules {
		versions[path] = nil
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return versions
	}
	for _, dep := range info.Deps {
		if _, tracked := versions[dep.Path]; tracked {
			versions[dep.Path] = dep.Version
		}
	}
	return versions
}

// gitCommit mirrors the Python lookup: the deployment's environment variable
// inside containers, git itself in a checkout.
func gitCommit() any {
	if commit := os.Getenv("ECCOM_GIT_COMMIT"); commit != "" {
		return commit
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil || len(strings.TrimSpace(string(out))) == 0 {
		return nil
	}
	return strings.TrimSpace(string(out))
}

func envOrNil(name string) any {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return nil
}
