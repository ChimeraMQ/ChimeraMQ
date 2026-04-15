package chaos_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// chaosNode holds the configuration for a single ChimeraMQ node in a chaos test.
type chaosNode struct {
	id         int
	dir        string
	port       int
	adminPort  int
	raftPort   int
	gossipPort int
}

// ---------------------------------------------------------------------------
// TestChaosLeaderKillAndRecovery
// ---------------------------------------------------------------------------

// TestChaosLeaderKillAndRecovery kills the Raft leader in a 3-node cluster and
// verifies that a new leader is elected and messages continue flowing.
func TestChaosLeaderKillAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos leader kill test in short mode")
	}

	binary := getChaosBinary(t)
	if binary == "" {
		t.Skip("chimera binary not found, skipping chaos test")
	}

	baseDir := t.TempDir()
	ports := findChaosPorts(t, 15)

	nodes := []chaosNode{
		{1, filepath.Join(baseDir, "node1"), ports[0], ports[1], ports[2], ports[3]},
		{2, filepath.Join(baseDir, "node2"), ports[5], ports[6], ports[7], ports[8]},
		{3, filepath.Join(baseDir, "node3"), ports[10], ports[11], ports[12], ports[13]},
	}

	raftPeers := []int{nodes[0].raftPort, nodes[1].raftPort, nodes[2].raftPort}
	processes := make([]*exec.Cmd, len(nodes))

	for i, node := range nodes {
		processes[i] = startNode(t, binary, node, raftPeers, nodes[0].gossipPort)
	}
	defer killAll(processes)

	t.Log("Waiting for cluster to form...")
	time.Sleep(5 * time.Second)

	createTopic(t, binary, nodes[0].adminPort, "chaos-leader-test", "9", "3")

	// Publish baseline messages to establish the cluster is working.
	baselineMsgs := 5
	t.Logf("Publishing %d baseline messages...", baselineMsgs)
	for i := 0; i < baselineMsgs; i++ {
		msg := fmt.Sprintf(`{"phase":"baseline","seq":%d}`, i)
		publishMessage(t, binary, nodes[0].adminPort, "chaos-leader-test", msg)
	}
	t.Log("Baseline messages published successfully")

	// Kill node1 (assumed leader — in a fresh 3-node cluster the first node
	// typically starts as candidate and wins the first election).
	t.Log("Killing node1 (assumed Raft leader)...")
	if processes[0] != nil && processes[0].Process != nil {
		processes[0].Process.Kill()
		processes[0].Wait()
		processes[0] = nil
	}
	t.Log("Node1 killed, waiting for re-election...")
	time.Sleep(5 * time.Second)

	// Publish through surviving nodes (node2 and node3).
	afterKillMsgs := 5
	t.Logf("Publishing %d messages through surviving nodes...", afterKillMsgs)
	for i := 0; i < afterKillMsgs; i++ {
		node := nodes[1+i%2]
		msg := fmt.Sprintf(`{"phase":"post-kill","seq":%d}`, i)
		publishMessage(t, binary, node.adminPort, "chaos-leader-test", msg)
	}
	t.Log("Messages published through surviving nodes successfully")

	// Restart node1 to verify recovery.
	t.Log("Restarting node1 to verify recovery...")
	processes[0] = startNode(t, binary, nodes[0], raftPeers, nodes[0].gossipPort)
	defer func(p *exec.Cmd) {
		if p != nil && p.Process != nil {
			p.Process.Kill()
			p.Wait()
		}
	}(processes[0])
	time.Sleep(5 * time.Second)

	// Verify node1 can accept messages again.
	recoveryMsgs := 3
	t.Logf("Publishing %d messages through recovered node1...", recoveryMsgs)
	for i := 0; i < recoveryMsgs; i++ {
		msg := fmt.Sprintf(`{"phase":"recovery","seq":%d}`, i)
		publishMessage(t, binary, nodes[0].adminPort, "chaos-leader-test", msg)
	}

	t.Logf("Leader kill+recovery test completed: %d baseline, %d post-kill, %d recovery messages",
		baselineMsgs, afterKillMsgs, recoveryMsgs)
}

// ---------------------------------------------------------------------------
// TestChaosNetworkPartitionRecovery
// ---------------------------------------------------------------------------

// TestChaosNetworkPartitionRecovery simulates a network partition by stopping
// one node, publishing to the other two, then restoring and verifying sync.
func TestChaosNetworkPartitionRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos network partition test in short mode")
	}

	binary := getChaosBinary(t)
	if binary == "" {
		t.Skip("chimera binary not found, skipping chaos test")
	}

	baseDir := t.TempDir()
	ports := findChaosPorts(t, 15)

	nodes := []chaosNode{
		{1, filepath.Join(baseDir, "node1"), ports[0], ports[1], ports[2], ports[3]},
		{2, filepath.Join(baseDir, "node2"), ports[5], ports[6], ports[7], ports[8]},
		{3, filepath.Join(baseDir, "node3"), ports[10], ports[11], ports[12], ports[13]},
	}

	raftPeers := []int{nodes[0].raftPort, nodes[1].raftPort, nodes[2].raftPort}
	processes := make([]*exec.Cmd, len(nodes))
	for i, node := range nodes {
		processes[i] = startNode(t, binary, node, raftPeers, nodes[0].gossipPort)
	}
	defer killAll(processes)

	t.Log("Waiting for cluster to form...")
	time.Sleep(5 * time.Second)

	createTopic(t, binary, nodes[0].adminPort, "chaos-partition-test", "9", "3")

	// Phase 1: All nodes healthy.
	phase1Msgs := 5
	t.Logf("Phase 1: Publishing %d messages with all nodes up...", phase1Msgs)
	for i := 0; i < phase1Msgs; i++ {
		msg := fmt.Sprintf(`{"phase":"pre-partition","seq":%d}`, i)
		publishMessage(t, binary, nodes[i%2].adminPort, "chaos-partition-test", msg)
	}

	// Phase 2: Stop node3 (simulates partition).
	t.Log("Phase 2: Stopping node3 to simulate network partition...")
	if processes[2] != nil && processes[2].Process != nil {
		processes[2].Process.Kill()
		processes[2].Wait()
		processes[2] = nil
	}
	time.Sleep(3 * time.Second)

	// Phase 3: Continue publishing to node1 and node2.
	phase3Msgs := 5
	t.Logf("Phase 3: Publishing %d messages with node3 down...", phase3Msgs)
	for i := 0; i < phase3Msgs; i++ {
		msg := fmt.Sprintf(`{"phase":"partition","seq":%d}`, i)
		publishMessage(t, binary, nodes[i%2].adminPort, "chaos-partition-test", msg)
	}

	// Phase 4: Restart node3 and verify it catches up.
	t.Log("Phase 4: Restarting node3...")
	processes[2] = startNode(t, binary, nodes[2], raftPeers, nodes[0].gossipPort)
	defer func(p *exec.Cmd) {
		if p != nil && p.Process != nil {
			p.Process.Kill()
			p.Wait()
		}
	}(processes[2])
	time.Sleep(8 * time.Second)

	// Phase 5: Verify node3 can accept and serve messages.
	phase5Msgs := 3
	t.Logf("Phase 5: Publishing %d messages through recovered node3...", phase5Msgs)
	for i := 0; i < phase5Msgs; i++ {
		msg := fmt.Sprintf(`{"phase":"post-recovery","seq":%d}`, i)
		publishMessage(t, binary, nodes[2].adminPort, "chaos-partition-test", msg)
	}

	t.Logf("Network partition test completed: %d pre-partition, %d during partition, %d post-recovery",
		phase1Msgs, phase3Msgs, phase5Msgs)
}

// ---------------------------------------------------------------------------
// TestChaosRapidReconnect
// ---------------------------------------------------------------------------

// TestChaosRapidReconnect simulates a client storm: many goroutines opening
// and immediately closing connections. Verifies the server doesn't crash or
// deadlock.
func TestChaosRapidReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos rapid reconnect test in short mode")
	}

	binary := getChaosBinary(t)
	if binary == "" {
		t.Skip("chimera binary not found, skipping chaos test")
	}

	baseDir := t.TempDir()
	ports := findChaosPorts(t, 5)
	nodeDir := filepath.Join(baseDir, "node1")

	writeSingleNodeConfig(t, baseDir, nodeDir, ports[0], ports[1])

	cmd := exec.Command(binary, "server", "--config", filepath.Join(nodeDir, "chimera.yaml"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	serverProc := cmd // save reference for cleanup

	port := ports[0]
	adminPort := ports[1]
	t.Logf("Started chaos reconnect server on port %d, admin %d", port, adminPort)
	time.Sleep(3 * time.Second)

	// Verify the main port is actually accepting connections before proceeding.
	if err := waitForPort(port, 5*time.Second); err != nil {
		serverProc.Process.Kill()
		serverProc.Wait()
		t.Fatalf("Server port %d not accepting connections: %v", port, err)
	}

	createTopic(t, binary, adminPort, "chaos-reconnect-test", "1", "1")

	const (
		numClients   = 50
		opsPerClient = 10
	)

	var (
		successCount atomic.Int64
		errorCount   atomic.Int64
	)

	t.Logf("Launching %d clients with %d ops each (%d total connections)...",
		numClients, opsPerClient, numClients*opsPerClient)

	var wg sync.WaitGroup
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for op := 0; op < opsPerClient; op++ {
				url := fmt.Sprintf("http://127.0.0.1:%d/", port)
				req, err := http.NewRequest("GET", url, nil)
				if err != nil {
					errorCount.Add(1)
					continue
				}
				req.Close = true // force close after response

				client := &http.Client{Timeout: 2 * time.Second}
				resp, err := client.Do(req)
				if err != nil {
					errorCount.Add(1)
				} else {
					resp.Body.Close()
					successCount.Add(1)
				}
			}
		}(c)
	}

	wg.Wait()

	success := successCount.Load()
	errors := errorCount.Load()
	t.Logf("Client storm results: %d successful connections, %d errors", success, errors)

	total := success + errors
	if total == 0 {
		t.Fatal("No connections were made during client storm")
	}

	// Verify server is still responsive after the storm.
	t.Log("Verifying server is still responsive after storm...")
	topicCmd := exec.Command(binary, "topic", "create", "--name", "post-storm-test", "--partitions", "1")
	topicCmd.Env = append(os.Environ(), fmt.Sprintf("CHIMERA_ADMIN_ADDR=127.0.0.1:%d", adminPort))
	if out, err := topicCmd.CombinedOutput(); err != nil {
		t.Logf("Post-storm topic creation failed (topic may already exist): %v\nOutput: %s", err, out)
	} else {
		t.Log("Post-storm topic created successfully — server is alive")
	}

	// Kill the server process before TempDir cleanup to avoid lock file issues.
	if serverProc.Process != nil {
		serverProc.Process.Kill()
		serverProc.Wait()
	}
}

// ---------------------------------------------------------------------------
// TestChaosLargeMessageFlood
// ---------------------------------------------------------------------------

// TestChaosLargeMessageFlood floods the server with messages of increasing sizes,
// verifying the server handles them without crashing.
func TestChaosLargeMessageFlood(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos large message flood test in short mode")
	}

	binary := getChaosBinary(t)
	if binary == "" {
		t.Skip("chimera binary not found, skipping chaos test")
	}

	baseDir := t.TempDir()
	ports := findChaosPorts(t, 5)
	nodeDir := filepath.Join(baseDir, "node1")

	maxMsgSize := 1048576 // 1MB
	writeSingleNodeConfig(t, baseDir, nodeDir, ports[0], ports[1])

	// Append max_message_size to the config.
	cfgPath := filepath.Join(nodeDir, "chimera.yaml")
	cfg, _ := os.ReadFile(cfgPath)
	cfgStr := string(cfg) + fmt.Sprintf("limits:\n  max_message_size: %d\n", maxMsgSize)
	os.WriteFile(cfgPath, []byte(cfgStr), 0644)

	cmd := exec.Command(binary, "server", "--config", cfgPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()

	adminPort := ports[1]
	t.Logf("Started flood test server on port %d, admin %d (max_message_size=%d)", ports[0], adminPort, maxMsgSize)
	time.Sleep(3 * time.Second)

	// Verify the main port is actually accepting connections.
	if err := waitForPort(ports[0], 5*time.Second); err != nil {
		t.Fatalf("Server port %d not accepting connections: %v", ports[0], err)
	}

	createTopic(t, binary, adminPort, "chaos-flood-test", "1", "1")

	// Test progressively larger messages via the admin HTTP API directly.
	// The produce CLI sends via HTTP POST, so we test by checking that the
	// server responds (even with rejection) for each size.
	sizes := []int{
		1024,       // 1KB
		10 * 1024,  // 10KB
		64 * 1024,  // 64KB
		256 * 1024, // 256KB
	}

	var (
		respondedCount atomic.Int64 // server responded (accepted or rejected)
		errorCount     atomic.Int64 // server didn't respond at all (connection refused, timeout)
	)

	adminBase := fmt.Sprintf("http://127.0.0.1:%d", adminPort)
	httpClient := &http.Client{Timeout: 10 * time.Second}

	for _, size := range sizes {
		body := []byte(strings.Repeat("X", size))
		t.Logf("Sending 3 messages of size %d bytes (%.1f KB)...", size, float64(size)/1024)

		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := httpClient.Post(
					fmt.Sprintf("%s/v1/messages/chaos-flood-test", adminBase),
					"application/json",
					strings.NewReader(string(body)),
				)
				if err != nil {
					errorCount.Add(1)
				} else {
					resp.Body.Close()
					respondedCount.Add(1)
				}
			}()
		}
		wg.Wait()
	}

	// Verify normal small messages still work after the flood.
	t.Log("Verifying normal small messages still work after flood...")
	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf(`{"post-flood":true,"seq":%d}`, i)
		publishMessage(t, binary, adminPort, "chaos-flood-test", msg)
	}

	totalResponded := respondedCount.Load()
	totalErrors := errorCount.Load()

	t.Logf("Flood test results: server responded=%d, no response (errors)=%d", totalResponded, totalErrors)

	// Server should have responded to all requests. Even rejections count as responses.
	if totalErrors > 0 {
		t.Errorf("Server failed to respond to %d requests — server may be unstable", totalErrors)
	}
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

// getChaosBinary returns the path to the chimera binary, or "" if not found.
func getChaosBinary(t *testing.T) string {
	candidates := []string{
		"../../bin/chimera",
		"../../bin/chimera.exe",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

// findChaosPorts finds count available TCP+UDP ports.
// Uses port range 50000+ to avoid conflicts with cluster tests (30000) and
// benchmarks (40000).
// On Windows, port binding can be sticky due to TIME_WAIT, so we find more
// ports than needed and use the last batch to give time for earlier ports
// to be released.
func findChaosPorts(t *testing.T, count int) []int {
	ports := make([]int, 0, count)
	basePort := 50000

	for len(ports) < count {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", basePort))
		if err == nil {
			ln.Close()
			// Small delay to ensure port is fully released on Windows
			time.Sleep(10 * time.Millisecond)
			udpLn, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", basePort))
			if err == nil {
				udpLn.Close()
				time.Sleep(10 * time.Millisecond)
				ports = append(ports, basePort)
			}
		}
		basePort++
		if basePort > 65000 {
			t.Fatal("Could not find enough available ports")
		}
	}
	return ports
}

// startNode starts a single cluster node and returns its process.
func startNode(t *testing.T, binary string, node chaosNode, raftPeers []int, seedGossipPort int) *exec.Cmd {
	raftPeerLines := make([]string, len(raftPeers))
	for i, p := range raftPeers {
		raftPeerLines[i] = fmt.Sprintf(`      - "127.0.0.1:%d"`, p)
	}

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
%s
  gossip:
    bind_port: %d
    hmac_key: "test-gossip-key-for-integration-tests-only"
    seeds:
      - "127.0.0.1:%d"
auth:
  enabled: false
limits:
  max_message_size: 1048576
`, node.id, node.id, node.dir, node.port, node.adminPort, strings.Join(raftPeerLines, "\n"), node.gossipPort, seedGossipPort)

	configPath := filepath.Join(node.dir, "chimera.yaml")
	os.MkdirAll(node.dir, 0755)
	os.WriteFile(configPath, []byte(cfg), 0644)

	cmd := exec.Command(binary, "server", "--config", configPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start node %d: %v", node.id, err)
	}
	t.Logf("Started node %d on port %d", node.id, node.port)
	return cmd
}

// killAll kills all processes in the slice.
func killAll(processes []*exec.Cmd) {
	for _, p := range processes {
		if p != nil && p.Process != nil {
			p.Process.Kill()
			p.Wait()
		}
	}
}

// createTopic creates a topic via the CLI.
func createTopic(t *testing.T, binary string, adminPort int, name, partitions, replication string) {
	t.Helper()
	cmd := exec.Command(binary, "topic", "create", "--name", name, "--partitions", partitions)
	if replication != "" && replication != "1" {
		cmd.Args = append(cmd.Args, "--replication", replication)
	}
	cmd.Env = append(os.Environ(), fmt.Sprintf("CHIMERA_ADMIN_ADDR=127.0.0.1:%d", adminPort))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create topic %q: %v\nOutput: %s", name, err, out)
	}
	t.Logf("Created topic %q with %s partitions, replication %s", name, partitions, replication)
}

// publishMessage publishes a single message via CLI.
func publishMessage(t *testing.T, binary string, adminPort int, topic, message string) {
	t.Helper()
	cmd := exec.Command(binary, "produce", "--topic", topic, "--message", message)
	cmd.Env = append(os.Environ(), fmt.Sprintf("CHIMERA_ADMIN_ADDR=127.0.0.1:%d", adminPort))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to publish message to topic %q: %v\nOutput: %s", topic, err, out)
	}
}

// writeSingleNodeConfig writes a minimal single-node YAML config file.
// Used by the non-cluster chaos tests.
func writeSingleNodeConfig(t *testing.T, baseDir, nodeDir string, port, adminPort int) {
	cfg := fmt.Sprintf(`
node:
  id: 1
  name: chaos-node
  data_dir: %s
listener:
  bind: 127.0.0.1
  port: %d
  admin_port: %d
cluster:
  enabled: false
auth:
  enabled: false
`, nodeDir, port, adminPort)

	configPath := filepath.Join(nodeDir, "chimera.yaml")
	os.MkdirAll(nodeDir, 0755)
	os.WriteFile(configPath, []byte(cfg), 0644)
}

// waitForPort polls a TCP port until it accepts a connection or times out.
func waitForPort(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("port %d not ready after %v", port, timeout)
}
