package environment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteProducesThePythonSchemaPlusGoFields(t *testing.T) {
	dir := t.TempDir()
	if err := Write("cloud", dir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "cloud-environment.json"))
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"host_role", "implementation", "hostname", "collected_at", "python_version", "go_version",
		"platform", "os", "kernel_version", "cpu", "open_files_limit", "git_commit",
		"docker_image_digest", "dependencies", "clock_sync",
	} {
		if _, ok := env[key]; !ok {
			t.Errorf("missing key %q", key)
		}
	}
	if env["host_role"] != "cloud" || env["implementation"] != "go" || env["python_version"] != nil {
		t.Errorf("identity fields: %v", env)
	}
	if !strings.HasPrefix(env["go_version"].(string), "go") {
		t.Errorf("go_version %v", env["go_version"])
	}
	cpu := env["cpu"].(map[string]any)
	if cpu["logical_cores"].(float64) < 1 {
		t.Errorf("cpu %v", cpu)
	}
	limits := env["open_files_limit"].(map[string]any)
	if _, ok := limits["soft"]; !ok {
		t.Errorf("open_files_limit %v", limits)
	}
	deps := env["dependencies"].(map[string]any)
	for _, module := range trackedModules {
		if _, ok := deps[module]; !ok {
			t.Errorf("dependency %q missing from %v", module, deps)
		}
	}
	if _, ok := cpu["container_limit_cores"]; !ok {
		t.Errorf("container_limit_cores missing from %v", cpu)
	}
}

func TestContainerCPULimitReadsBothCgroupVersions(t *testing.T) {
	root := t.TempDir()
	if got := containerCPULimit(root); got != nil {
		t.Errorf("no cgroup files: want nil, got %v", got)
	}
	if err := os.WriteFile(filepath.Join(root, "cpu.max"), []byte("600000 100000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := containerCPULimit(root); got != 6.0 {
		t.Errorf("cgroup v2: want 6, got %v", got)
	}
	if err := os.WriteFile(filepath.Join(root, "cpu.max"), []byte("max 100000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := containerCPULimit(root); got != nil {
		t.Errorf("cgroup v2 unlimited: want nil, got %v", got)
	}
	v1 := t.TempDir()
	if err := os.MkdirAll(filepath.Join(v1, "cpu"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(v1, "cpu", "cpu.cfs_quota_us"), []byte("350000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(v1, "cpu", "cpu.cfs_period_us"), []byte("100000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := containerCPULimit(v1); got != 3.5 {
		t.Errorf("cgroup v1: want 3.5, got %v", got)
	}
}
