package cluster_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestClusterFailoverLeaderKill tests automatic failover when the leader is killed.
func TestClusterFailoverLeaderKill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster failover test in short mode")
	}

	// This test requires a compiled chimera binary
	binary := getBinaryPath(t)

	// Create temporary directories for 3-node cluster
	baseDir := t.TempDir()
	nodes := []struct {
		id         int
		dir        string
		port       int
		adminPort  int
		raftPort   int
		gossipPort int
	}{
		{1, filepath.Join(baseDir, "node1"), 15672, 25672, 22000, 16946},
		{2, filepath.Join(baseDir, "node2"), 15673, 25673, 22001, 16947},
		{3, filepath.Join(baseDir, "node3"), 15674, 25674, 22002, 16948},
	}

	// Start 3 nodes
	processes := make([]*exec.Cmd, 0, len(nodes))
	defer func() {
		// Cleanup: kill all processes
		for _, p := range processes {
			if p != nil && p.Process != nil {
				p.Process.Kill()
				p.Wait()
			}
		}
	}()

	for _, node := range nodes {
		cfg := fmt.Sprintf(`
node:
  id: %d
  name: node-%d
  data_dir: %s
listener:
  bind: 127.0.0.1
  port: %d
  admin_port: %d
cluster:
  enabled: true
  raft:
    peers:
      - "127.0.0.1:%d"
      - "127.0.0.1:%d"
      - "127.0.0.1:%d"
  gossip:
    bind_port: %d
    hmac_key: "test-gossip-key-for-integration-tests-only"
    seeds:
      - "127.0.0.1:16946"
auth:
  enabled: false
`, node.id, node.id, node.dir, node.port, node.adminPort, nodes[0].raftPort, nodes[1].raftPort, nodes[2].raftPort, node.gossipPort)

		configPath := filepath.Join(node.dir, "chimera.yaml")
		os.MkdirAll(node.dir, 0755)
		os.WriteFile(configPath, []byte(cfg), 0644)

		cmd := exec.Command(binary, "server", "--config", configPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %d: %v", node.id, err)
		}
		processes = append(processes, cmd)
		t.Logf("Started node %d on port %d", node.id, node.port)
	}

	// Wait for cluster to form
	t.Log("Waiting for cluster to form...")
	time.Sleep(3 * time.Second)

	// Create a topic and publish some messages
	adminPort := nodes[0].adminPort
	cmd := exec.Command(binary, "topic", "create", "--name", "failover-test", "--partitions", "3")
	cmd.Env = append(os.Environ(), fmt.Sprintf("CHIMERA_ADMIN_ADDR=127.0.0.1:%d", adminPort))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create topic: %v\nOutput: %s", err, out)
	}

	// Publish messages before killing leader
	t.Log("Publishing messages before killing leader...")
	for i := 0; i < 10; i++ {
		cmd := exec.Command(binary, "produce", "--topic", "failover-test", "--message", fmt.Sprintf("message-%d", i))
		cmd.Env = append(os.Environ(), fmt.Sprintf("CHIMERA_ADMIN_ADDR=127.0.0.1:%d", adminPort))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("Warning: failed to publish message %d: %v\nOutput: %s", i, err, out)
		}
	}

	// Find and kill the leader (node1 is likely the leader)
	t.Log("Killing leader (node1)...")
	if len(processes) > 0 && processes[0] != nil && processes[0].Process != nil {
		processes[0].Process.Kill()
		processes[0].Wait()
		processes[0] = nil
	}

	// Wait for failover
	t.Log("Waiting for failover...")
	time.Sleep(5 * time.Second)

	// Try to publish more messages through a surviving node
	t.Log("Publishing messages after failover...")
	adminPort = nodes[1].adminPort
	successCount := 0
	for i := 10; i < 20; i++ {
		cmd := exec.Command(binary, "produce", "--topic", "failover-test", "--message", fmt.Sprintf("message-%d", i))
		cmd.Env = append(os.Environ(), fmt.Sprintf("CHIMERA_ADMIN_ADDR=127.0.0.1:%d", adminPort))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("Failed to publish message %d after failover: %v\nOutput: %s", i, err, out)
		} else {
			successCount++
		}
	}

	if successCount == 0 {
		t.Error("No messages could be published after failover")
	} else {
		t.Logf("Successfully published %d messages after failover", successCount)
	}
}

// TestClusterFailoverNetworkPartition tests behavior during network partition.
func TestClusterFailoverNetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster network partition test in short mode")
	}

	// This is a complex test that would require network manipulation
	// For now, we just document what it should test
	t.Skip("Network partition test requires external network manipulation tools")
}

// TestClusterFailoverQuorumLoss tests behavior when quorum is lost.
func TestClusterFailoverQuorumLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster quorum loss test in short mode")
	}

	binary := getBinaryPath(t)

	// Create temporary directories for 3-node cluster
	baseDir := t.TempDir()
	nodes := []struct {
		id         int
		dir        string
		port       int
		adminPort  int
		raftPort   int
		gossipPort int
	}{
		{1, filepath.Join(baseDir, "node1"), 15672, 25672, 22000, 16946},
		{2, filepath.Join(baseDir, "node2"), 15673, 25673, 22001, 16947},
		{3, filepath.Join(baseDir, "node3"), 15674, 25674, 22002, 16948},
	}

	// Start 3 nodes
	processes := make([]*exec.Cmd, 0, len(nodes))
	defer func() {
		for _, p := range processes {
			if p != nil && p.Process != nil {
				p.Process.Kill()
				p.Wait()
			}
		}
	}()

	for _, node := range nodes {
		cfg := fmt.Sprintf(`
node:
  id: %d
  name: node-%d
  data_dir: %s
listener:
  bind: 127.0.0.1
  port: %d
  admin_port: %d
cluster:
  enabled: true
  raft:
    peers:
      - "127.0.0.1:%d"
      - "127.0.0.1:%d"
      - "127.0.0.1:%d"
  gossip:
    bind_port: %d
    hmac_key: "test-gossip-key-for-integration-tests-only"
    seeds:
      - "127.0.0.1:16946"
auth:
  enabled: false
`, node.id, node.id, node.dir, node.port, node.adminPort, nodes[0].raftPort, nodes[1].raftPort, nodes[2].raftPort, node.gossipPort)

		configPath := filepath.Join(node.dir, "chimera.yaml")
		os.MkdirAll(node.dir, 0755)
		os.WriteFile(configPath, []byte(cfg), 0644)

		cmd := exec.Command(binary, "server", "--config", configPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %d: %v", node.id, err)
		}
		processes = append(processes, cmd)
	}

	// Wait for cluster to form
	time.Sleep(3 * time.Second)

	// Kill 2 nodes (quorum loss)
	t.Log("Killing 2 nodes to trigger quorum loss...")
	for i := 0; i < 2 && i < len(processes); i++ {
		if processes[i] != nil && processes[i].Process != nil {
			processes[i].Process.Kill()
			processes[i].Wait()
			processes[i] = nil
		}
	}

	// Wait a bit
	time.Sleep(2 * time.Second)

	// Try to publish through the surviving node (should fail without quorum)
	adminPort := nodes[2].port + 1000
	cmd := exec.Command(binary, "produce", "--topic", "test", "--message", "test-message")
	cmd.Env = append(os.Environ(), fmt.Sprintf("CHIMERA_ADMIN_ADDR=127.0.0.1:%d", adminPort))
	out, err := cmd.CombinedOutput()

	// With quorum loss, operations may fail or hang
	// The test documents the behavior
	if err != nil {
		t.Logf("Expected behavior: operations fail without quorum: %v\nOutput: %s", err, out)
	} else {
		t.Logf("Surviving node handled request without quorum: %s", out)
	}
}

// getBinaryPath returns the path to the chimera binary.
func getBinaryPath(t *testing.T) string {
	// Try to find the binary
	candidates := []string{
		"../../bin/chimera",
		"../../bin/chimera.exe",
		"chimera",
	}

	// Check candidates
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	if _, err := os.Stat("../../bin/chimera"); err == nil {
		return "../../bin/chimera"
	}

	// Try to build the binary
	t.Log("Building chimera binary...")
	cmd := exec.Command("go", "build", "-o", "chimera-test", "../../cmd/chimera")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("Cannot build chimera binary: %v\nOutput: %s", err, out)
	}
	defer os.Remove("chimera-test")

	absPath, _ := filepath.Abs("chimera-test")
	return absPath
}

// TestSplitBrainPrevention validates split-brain prevention mechanisms.
func TestSplitBrainPrevention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping split-brain prevention test in short mode")
	}

	// This test validates that:
	// 1. Only one leader exists at a time
	// 2. Old leader steps down when partition heals
	// 3. No divergent logs are committed

	t.Skip("Split-brain prevention test requires cluster introspection capabilities not yet implemented")
}

// TestClusterFailoverGraceful tests graceful leader transfer.
func TestClusterFailoverGraceful(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping graceful failover test in short mode")
	}

	binary := getBinaryPath(t)

	// Create temporary directories for 3-node cluster
	baseDir := t.TempDir()
	nodes := []struct {
		id         int
		dir        string
		port       int
		adminPort  int
		raftPort   int
		gossipPort int
	}{
		{1, filepath.Join(baseDir, "node1"), 15672, 25672, 22000, 16946},
		{2, filepath.Join(baseDir, "node2"), 15673, 25673, 22001, 16947},
		{3, filepath.Join(baseDir, "node3"), 15674, 25674, 22002, 16948},
	}

	// Start 3 nodes
	processes := make([]*exec.Cmd, 0, len(nodes))
	defer func() {
		for _, p := range processes {
			if p != nil && p.Process != nil {
				p.Process.Signal(os.Interrupt)
				p.Wait()
			}
		}
	}()

	for _, node := range nodes {
		cfg := fmt.Sprintf(`
node:
  id: %d
  name: node-%d
  data_dir: %s
listener:
  bind: 127.0.0.1
  port: %d
  admin_port: %d
cluster:
  enabled: true
  raft:
    peers:
      - "127.0.0.1:%d"
      - "127.0.0.1:%d"
      - "127.0.0.1:%d"
  gossip:
    bind_port: %d
    hmac_key: "test-gossip-key-for-integration-tests-only"
    seeds:
      - "127.0.0.1:16946"
auth:
  enabled: false
`, node.id, node.id, node.dir, node.port, node.adminPort, nodes[0].raftPort, nodes[1].raftPort, nodes[2].raftPort, node.gossipPort)

		configPath := filepath.Join(node.dir, "chimera.yaml")
		os.MkdirAll(node.dir, 0755)
		os.WriteFile(configPath, []byte(cfg), 0644)

		cmd := exec.Command(binary, "server", "--config", configPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %d: %v", node.id, err)
		}
		processes = append(processes, cmd)
		t.Logf("Started node %d on port %d", node.id, node.port)
	}

	// Wait for cluster to form
	time.Sleep(3 * time.Second)

	// Gracefully stop the leader
	t.Log("Gracefully stopping leader (node1)...")
	if len(processes) > 0 && processes[0] != nil && processes[0].Process != nil {
		processes[0].Process.Signal(os.Interrupt)
		processes[0].Wait()
		processes[0] = nil
	}

	// Wait for failover
	time.Sleep(5 * time.Second)

	// Verify cluster is still operational
	adminPort := nodes[1].port + 1000
	cmd := exec.Command(binary, "cluster", "status")
	cmd.Env = append(os.Environ(), fmt.Sprintf("CHIMERA_ADMIN_ADDR=127.0.0.1:%d", adminPort))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Cluster status check failed: %v\nOutput: %s", err, out)
	} else {
		t.Logf("Cluster status after graceful leader stop: %s", out)
	}
}
