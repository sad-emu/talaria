package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"talaria/config"
)

// BuildServerTLSConfig returns a *tls.Config suitable for a talaria listener.
// It requires mutual TLS and enforces the DN allowlist on each connection.
func BuildServerTLSConfig(cfg *config.TLSConfig) (*tls.Config, error) {
	cert, caPool, err := loadMaterial(cfg)
	if err != nil {
		return nil, err
	}
	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	if len(cfg.AllowedDNs) > 0 {
		allowed := cfg.AllowedDNs
		tc.VerifyConnection = func(cs tls.ConnectionState) error {
			return verifyDN(cs, allowed)
		}
	}
	return tc, nil
}

// BuildClientTLSConfig returns a *tls.Config suitable for a talaria dialer.
// It presents the node's own certificate and verifies the server against the CA
// and DN allowlist.
func BuildClientTLSConfig(cfg *config.TLSConfig) (*tls.Config, error) {
	cert, caPool, err := loadMaterial(cfg)
	if err != nil {
		return nil, err
	}
	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
	}
	if len(cfg.AllowedDNs) > 0 {
		allowed := cfg.AllowedDNs
		tc.VerifyConnection = func(cs tls.ConnectionState) error {
			return verifyDN(cs, allowed)
		}
	}
	return tc, nil
}

// loadMaterial loads the certificate/key pair and CA pool from cfg.
func loadMaterial(cfg *config.TLSConfig) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("tls: load cert/key: %w", err)
	}
	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("tls: read CA file: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, fmt.Errorf("tls: no valid CA certificates found in %q", cfg.CAFile)
	}
	return cert, caPool, nil
}

// verifyDN checks that the peer's leaf certificate Subject DN contains at least
// one of the allowed substrings.  Called as a VerifyConnection callback after
// normal chain verification has already succeeded.
func verifyDN(cs tls.ConnectionState, allowedDNs []string) error {
	if len(cs.PeerCertificates) == 0 {
		return fmt.Errorf("tls: no peer certificate presented")
	}
	peerDN := cs.PeerCertificates[0].Subject.String()
	for _, allowed := range allowedDNs {
		if strings.Contains(peerDN, allowed) {
			return nil
		}
	}
	return fmt.Errorf("tls: peer DN %q not in allowed list", peerDN)
}
