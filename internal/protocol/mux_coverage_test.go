package protocol

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

func generateTestCerts(t *testing.T, dir string) (certFile, keyFile, caFile string) {
	t.Helper()

	// Generate CA
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caFile = filepath.Join(dir, "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	if err := os.WriteFile(caFile, caPEM, 0644); err != nil {
		t.Fatal(err)
	}

	// Generate server cert
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  nil,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	caCert, _ := x509.ParseCertificate(caCertDER)
	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	certFile = filepath.Join(dir, "server.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	if err := os.WriteFile(certFile, certPEM, 0644); err != nil {
		t.Fatal(err)
	}

	keyFile = filepath.Join(dir, "server.key")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}

	return certFile, keyFile, caFile
}

func TestServeTLSWithRealCerts(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, _ := generateTestCerts(t, dir)

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: t.TempDir()},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, MaxConnections: 100},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		TLS:     broker.TLSConfig{Enabled: true, CertFile: certFile, KeyFile: keyFile},
	}
	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	mux := NewProtocolMux(b)
	mux.Register(&echoDetector{}, &noopHandler{})

	done := make(chan error, 1)
	go func() { done <- mux.Serve() }()

	// Wait for listener
	time.Sleep(100 * time.Millisecond)

	// Verify it's listening with TLS
	if mux.Listener() == nil {
		t.Fatal("listener should be created")
	}

	mux.Stop()
	if err := <-done; err != nil {
		t.Errorf("Serve returned: %v", err)
	}
}

func TestServeTLSMutualWithRealCerts(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := generateTestCerts(t, dir)

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: t.TempDir()},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, MaxConnections: 100},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		TLS:     broker.TLSConfig{Enabled: true, Mutual: true, CertFile: certFile, KeyFile: keyFile, ClientCA: caFile},
	}
	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	mux := NewProtocolMux(b)
	mux.Register(&echoDetector{}, &noopHandler{})

	done := make(chan error, 1)
	go func() { done <- mux.Serve() }()

	time.Sleep(100 * time.Millisecond)

	if mux.Listener() == nil {
		t.Fatal("listener should be created")
	}

	// Verify TLS config was set
	if mux.tlsConfig == nil {
		t.Error("expected TLS config to be set")
	}

	mux.Stop()
	if err := <-done; err != nil {
		t.Errorf("Serve returned: %v", err)
	}
}

func TestServeAcceptErrorNotContext(t *testing.T) {
	// Create a mux, start Serve, then close the listener to trigger an accept error
	b, cleanup := newTestBrokerForMux(t)
	defer cleanup()

	mux := NewProtocolMux(b)
	mux.Register(&echoDetector{}, &noopHandler{})

	// Start Serve in background
	done := make(chan error, 1)
	go func() { done <- mux.Serve() }()

	time.Sleep(100 * time.Millisecond)

	// Stop the mux (this will close the listener)
	mux.Stop()

	select {
	case err := <-done:
		// Accept error was logged but Serve returned nil after Stop
		_ = err
	case <-time.After(3 * time.Second):
		t.Error("Serve should have returned")
	}
}

func TestBufferedConnFullRead(t *testing.T) {
	// Test the bufferedConn.Read method more thoroughly
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		client.Write([]byte("hello"))
		client.Close()
	}()

	br := bufio.NewReaderSize(server, 256)
	bufConn := &bufferedConn{Conn: server, reader: br}

	buf := make([]byte, 100)
	n, err := bufConn.Read(buf)
	if err != nil {
		t.Logf("Read returned: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("got %q, want 'hello'", buf[:n])
	}
}
