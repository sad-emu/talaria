package tls

import (
	"crypto/rand"
	"crypto/rsa"
	cryptotls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"talaria/config"
)

// ---------------------------------------------------------------------------
// Test certificate helpers
// ---------------------------------------------------------------------------

type testCerts struct {
	caPEM      []byte
	serverCert []byte
	serverKey  []byte
	clientCert []byte
	clientKey  []byte
}

func generateTestCerts(t *testing.T, clientCN string) testCerts {
	t.Helper()

	// CA
	caKey, err := rsa.GenerateKey(rand.Reader, 1024) // 1024-bit is fine for tests
	if err != nil {
		t.Fatalf("gen CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "TestCA", Organization: []string{"TalariaTest"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	makeNode := func(cn string, serial int64) (certPEM, keyPEM []byte) {
		key, _ := rsa.GenerateKey(rand.Reader, 1024) // 1024-bit is fine for tests
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject: pkix.Name{
				CommonName:   cn,
				Organization: []string{"TalariaTest"},
			},
			NotBefore:   time.Now().Add(-time.Hour),
			NotAfter:    time.Now().Add(24 * time.Hour),
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		}
		certDER, _ := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
		keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		return
	}

	serverCert, serverKey := makeNode("talaria-server", 2)
	clientCert, clientKey := makeNode(clientCN, 3)

	return testCerts{
		caPEM:      caPEM,
		serverCert: serverCert,
		serverKey:  serverKey,
		clientCert: clientCert,
		clientKey:  clientKey,
	}
}

func writeCertFiles(t *testing.T, certs testCerts) (caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile string) {
	t.Helper()
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		p := dir + "/" + name
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}
	return write("ca.crt", certs.caPEM),
		write("server.crt", certs.serverCert),
		write("server.key", certs.serverKey),
		write("client.crt", certs.clientCert),
		write("client.key", certs.clientKey)
}

// ---------------------------------------------------------------------------
// verifyDN tests
// ---------------------------------------------------------------------------

func TestVerifyDN_Allowed(t *testing.T) {
	certs := generateTestCerts(t, "talaria-node-2")
	caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile := writeCertFiles(t, certs)

	serverCfg := &config.TLSConfig{
		CertFile:   serverCertFile,
		KeyFile:    serverKeyFile,
		CAFile:     caFile,
		AllowedDNs: []string{"talaria-node-2"},
	}
	clientCfg := &config.TLSConfig{
		CertFile:   clientCertFile,
		KeyFile:    clientKeyFile,
		CAFile:     caFile,
		AllowedDNs: []string{"talaria-server"},
	}

	handshakeErr := runHandshake(t, serverCfg, clientCfg)
	if handshakeErr != nil {
		t.Errorf("expected successful handshake, got: %v", handshakeErr)
	}
}

func TestVerifyDN_Denied(t *testing.T) {
	certs := generateTestCerts(t, "talaria-node-2")
	caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile := writeCertFiles(t, certs)

	// Server only allows "talaria-node-WRONG", so the client will be rejected.
	serverCfg := &config.TLSConfig{
		CertFile:   serverCertFile,
		KeyFile:    serverKeyFile,
		CAFile:     caFile,
		AllowedDNs: []string{"talaria-node-WRONG"},
	}
	clientCfg := &config.TLSConfig{
		CertFile: clientCertFile,
		KeyFile:  clientKeyFile,
		CAFile:   caFile,
	}

	handshakeErr := runHandshake(t, serverCfg, clientCfg)
	if handshakeErr == nil {
		t.Error("expected handshake failure due to DN mismatch, got nil")
	}
}

func TestVerifyDN_NoPeerCertificates(t *testing.T) {
	cs := cryptotls.ConnectionState{} // PeerCertificates is nil
	err := verifyDN(cs, []string{"anything"})
	if err == nil {
		t.Fatal("expected error for empty PeerCertificates, got nil")
	}
}

func TestVerifyDN_SubstringMatch(t *testing.T) {
	certs := generateTestCerts(t, "talaria-node-2")
	caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile := writeCertFiles(t, certs)

	// Partial substring should still match.
	serverCfg := &config.TLSConfig{
		CertFile:   serverCertFile,
		KeyFile:    serverKeyFile,
		CAFile:     caFile,
		AllowedDNs: []string{"node-2"}, // partial
	}
	clientCfg := &config.TLSConfig{
		CertFile: clientCertFile,
		KeyFile:  clientKeyFile,
		CAFile:   caFile,
	}

	if err := runHandshake(t, serverCfg, clientCfg); err != nil {
		t.Errorf("expected success with partial DN substring, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// BuildServerTLSConfig / BuildClientTLSConfig error path tests
// ---------------------------------------------------------------------------

func TestBuildServerTLSConfig_MissingCert(t *testing.T) {
	cfg := &config.TLSConfig{
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
		CAFile:   "/nonexistent/ca.pem",
	}
	_, err := BuildServerTLSConfig(cfg)
	if err == nil {
		t.Fatal("expected error for missing cert files")
	}
}

func TestBuildClientTLSConfig_MissingCert(t *testing.T) {
	cfg := &config.TLSConfig{
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
		CAFile:   "/nonexistent/ca.pem",
	}
	_, err := BuildClientTLSConfig(cfg)
	if err == nil {
		t.Fatal("expected error for missing cert files")
	}
}

func TestBuildServerTLSConfig_InvalidCA(t *testing.T) {
	certs := generateTestCerts(t, "node")
	dir := t.TempDir()
	serverCertFile := dir + "/server.crt"
	serverKeyFile := dir + "/server.key"
	caFile := dir + "/ca.crt"
	os.WriteFile(serverCertFile, certs.serverCert, 0600)
	os.WriteFile(serverKeyFile, certs.serverKey, 0600)
	os.WriteFile(caFile, []byte("not a valid PEM"), 0600) // garbage CA

	cfg := &config.TLSConfig{
		CertFile: serverCertFile,
		KeyFile:  serverKeyFile,
		CAFile:   caFile,
	}
	_, err := BuildServerTLSConfig(cfg)
	if err == nil {
		t.Fatal("expected error for invalid CA PEM")
	}
}

func TestBuildServerTLSConfig_NoAllowedDNs(t *testing.T) {
	certs := generateTestCerts(t, "node")
	caFile, serverCertFile, serverKeyFile, clientCertFile, clientKeyFile := writeCertFiles(t, certs)

	// Without AllowedDNs, any valid cert signed by the CA should be accepted.
	serverCfg := &config.TLSConfig{
		CertFile: serverCertFile,
		KeyFile:  serverKeyFile,
		CAFile:   caFile,
	}
	clientCfg := &config.TLSConfig{
		CertFile: clientCertFile,
		KeyFile:  clientKeyFile,
		CAFile:   caFile,
	}

	if err := runHandshake(t, serverCfg, clientCfg); err != nil {
		t.Errorf("expected success without AllowedDNs, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Integration: full TLS handshake over net.Pipe()
// ---------------------------------------------------------------------------

// runHandshake performs a mutual TLS handshake between a server and client
// built from the given configs.  Returns the first error from either side.
func runHandshake(t *testing.T, serverCfgIn, clientCfgIn *config.TLSConfig) error {
	t.Helper()

	serverTLS, err := BuildServerTLSConfig(serverCfgIn)
	if err != nil {
		t.Fatalf("BuildServerTLSConfig: %v", err)
	}
	clientTLS, err := BuildClientTLSConfig(clientCfgIn)
	if err != nil {
		t.Fatalf("BuildClientTLSConfig: %v", err)
	}
	// ServerName must match a SAN in the server cert; we use the IP SAN.
	clientTLS.ServerName = "127.0.0.1"

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { serverConn.Close(); clientConn.Close() })

	// Set a deadline on both sides to prevent unbuffered-pipe deadlocks when
	// one side rejects the handshake and tries to write an alert.
	deadline := time.Now().Add(10 * time.Second)
	serverConn.SetDeadline(deadline)
	clientConn.SetDeadline(deadline)

	errs := make(chan error, 2)
	go func() {
		s := cryptotls.Server(serverConn, serverTLS)
		errs <- s.Handshake()
		serverConn.Close() // close raw conn — avoids TLS close_notify deadlock on net.Pipe
	}()
	go func() {
		c := cryptotls.Client(clientConn, clientTLS)
		errs <- c.Handshake()
		clientConn.Close() // close raw conn — avoids TLS close_notify deadlock on net.Pipe
	}()

	var first error
	for i := 0; i < 2; i++ {
		if e := <-errs; e != nil && first == nil {
			first = e
		}
	}
	return first
}
