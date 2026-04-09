package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()
	if cfg.Node.ID != 1 {
		t.Errorf("expected node ID 1, got %d", cfg.Node.ID)
	}
	if cfg.Listener.Port != 5672 {
		t.Errorf("expected port 5672, got %d", cfg.Listener.Port)
	}
	if cfg.Storage.Hot.SegmentSize != 256*1024*1024 {
		t.Errorf("expected 256MB segment size")
	}
	if cfg.Defaults.Topic.Mode != "unified" {
		t.Errorf("expected unified mode")
	}
}

func TestLoadConfigFromYAML(t *testing.T) {
	yamlContent := `
node:
  id: 5
  name: "test-node"
  data_dir: "/tmp/chimera-test"
listener:
  port: 6666
  admin_port: 8888
storage:
  hot:
    segment_size: 134217728
    sync_mode: "immediate"
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "chimera.yaml")
	os.WriteFile(cfgPath, []byte(yamlContent), 0644)

	cfg, err := LoadConfig(cfgPath, nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Node.ID != 5 {
		t.Errorf("expected node ID 5, got %d", cfg.Node.ID)
	}
	if cfg.Listener.Port != 6666 {
		t.Errorf("expected port 6666, got %d", cfg.Listener.Port)
	}
	if cfg.Storage.Hot.SyncMode != "immediate" {
		t.Errorf("expected immediate sync")
	}
	// Defaults should still apply for non-overridden fields
	if cfg.Listener.AdminPort != 8888 {
		t.Errorf("expected admin port 8888, got %d", cfg.Listener.AdminPort)
	}
}

func TestEnvOverrides(t *testing.T) {
	os.Setenv("CHIMERA_NODE_ID", "42")
	os.Setenv("CHIMERA_LISTEN_PORT", "7777")
	defer os.Unsetenv("CHIMERA_NODE_ID")
	defer os.Unsetenv("CHIMERA_LISTEN_PORT")

	cfg, err := LoadConfig("", nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Node.ID != 42 {
		t.Errorf("expected node ID 42, got %d", cfg.Node.ID)
	}
	if cfg.Listener.Port != 7777 {
		t.Errorf("expected port 7777, got %d", cfg.Listener.Port)
	}
}

func TestCLIOverrides(t *testing.T) {
	flags := &CLIFlags{
		DataDir:   "/custom/dir",
		Port:      9999,
		AdminPort: 8080,
	}
	cfg, err := LoadConfig("", flags)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Node.DataDir != "/custom/dir" {
		t.Errorf("expected /custom/dir, got %s", cfg.Node.DataDir)
	}
	if cfg.Listener.Port != 9999 {
		t.Errorf("expected port 9999, got %d", cfg.Listener.Port)
	}
	if cfg.Listener.AdminPort != 8080 {
		t.Errorf("expected admin port 8080, got %d", cfg.Listener.AdminPort)
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name:    "valid defaults",
			modify:  func(c *Config) {},
			wantErr: false,
		},
		{
			name:    "invalid port",
			modify:  func(c *Config) { c.Listener.Port = 0 },
			wantErr: true,
		},
		{
			name:    "invalid admin port",
			modify:  func(c *Config) { c.Listener.AdminPort = 99999 },
			wantErr: true,
		},
		{
			name:    "empty data dir",
			modify:  func(c *Config) { c.Node.DataDir = "" },
			wantErr: true,
		},
		{
			name:    "invalid sync mode",
			modify:  func(c *Config) { c.Storage.Hot.SyncMode = "bad" },
			wantErr: true,
		},
		{
			name:    "invalid WAL sync mode",
			modify:  func(c *Config) { c.Storage.WAL.SyncMode = "bad" },
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPartialYAML(t *testing.T) {
	yamlContent := `
node:
  id: 3
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "chimera.yaml")
	os.WriteFile(cfgPath, []byte(yamlContent), 0644)

	cfg, err := LoadConfig(cfgPath, nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Node.ID != 3 {
		t.Errorf("expected node ID 3")
	}
	// Defaults should fill in
	if cfg.Listener.Port != 5672 {
		t.Errorf("expected default port 5672")
	}
}
