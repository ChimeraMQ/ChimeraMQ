# Running ChimeraMQ on Windows

ChimeraMQ is written in pure Go (no CGo) and runs natively on Windows. The
single `chimera.exe` binary requires no external dependencies.

## Installation

```powershell
# Download the Windows release
chimera.exe server --config chimera.yaml
```

Or build from source:

```powershell
make build
# or directly:
go build -ldflags="-s -w" -o bin/chimera.exe ./cmd/chimera
```

## Signal Handling

Windows does not support POSIX signals. ChimeraMQ handles this gracefully:

- **SIGINT (Ctrl+C)** — Works normally; triggers graceful shutdown
- **SIGTERM** — Not sent by the Windows OS. To stop the server, use Ctrl+C
  or `taskkill /PID <pid>` which sends a console control event

The 30-second shutdown timeout applies on all platforms.

## Data Directory Lock File

ChimeraMQ uses a `chimera.lock` file in the data directory to prevent
multiple instances from running on the same data. On Windows:

- Lock files use `O_EXCL` semantics (atomic creation via `CREATE_NEW`)
- Stale lock detection reads the PID and checks if the process is alive
  via `FindProcess` + `Signal(0)` — this works on Windows for local
  processes
- If a process crashes without releasing the lock, delete the
  `chimera.lock` file manually before restarting

## Known Limitations

### Cluster Tests

The cluster integration tests (`test/cluster/`) may occasionally fail on
Windows due to:

1. **UDP port binding delays** — Windows can be slow to release UDP ports
   after a process exits. The gossip protocol uses UDP and may need a few
   seconds between test runs.
2. **Process spawn overhead** — Tests use `exec.Command` per message for
   isolation. On Windows, process creation is ~3-5x slower than Linux,
   limiting throughput to ~30 msg/s in load tests.
3. **File handle contention** — Multiple nodes writing to the same base
   temp directory can trigger brief file lock conflicts.

**Recommendation:** Run cluster tests individually with a longer timeout:

```powershell
go test ./test/cluster/ -v -count=1 -timeout 300s
```

### Chaos Tests

The chaos tests (`test/chaos/`) may need longer timeouts on Windows due to
slower process termination and restart cycles.

### File System Semantics

- ChimeraMQ does **not** use `mmap` — all file I/O uses standard Go
  `os.ReadFile`, `os.WriteFile`, and `io` primitives, which behave
  consistently on Windows
- Segment files in hot storage are flushed with `Sync()` after each batch
- WAL entries use `O_DSYNC`-equivalent flushing on Windows

### Performance

- TCP connection handling is comparable to Linux on modern Windows
- Hot storage throughput may be 10-20% lower due to NTFS overhead
- For production workloads, consider running under WSL2 for Linux file
  system semantics

## PowerShell Quick Start

```powershell
# Start a single-node server
.\chimera.exe server --config chimera.yaml

# Create a topic
$env:CHIMERA_ADMIN_ADDR = "127.0.0.1:9090"
.\chimera.exe topic create --name my-topic --partitions 3

# Publish a message
.\chimera.exe produce --topic my-topic --message "Hello from Windows!"

# Consume messages
.\chimera.exe consume --topic my-topic --from-beginning
```

## Configuration Example

```yaml
node:
  id: 1
  name: chimera-windows
  data_dir: C:\chimera\data
listener:
  bind: 127.0.0.1
  port: 5672
  admin_port: 9090
auth:
  enabled: false
cluster:
  enabled: false
```

## Troubleshooting

### "data directory locked" error

1. Check if another `chimera.exe` is running: `Get-Process chimera`
2. If no process is running, delete `C:\chimera\data\chimera.lock`
3. Restart the server

### "address already in use"

Windows port binding can be sticky. Run:

```powershell
netstat -ano | findstr :5672
```

Kill any process using the port, or change the port in config.

### Cluster node can't connect to peers

Ensure Windows Firewall allows connections on the Raft, gossip, and
listener ports, or bind to `127.0.0.1` for local-only clusters.
