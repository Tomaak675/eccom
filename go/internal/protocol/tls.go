package protocol

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"

	"eccom/internal/config"
)

// serverTLSConfig loads the receiver's certificate and key (HTTPS).
// Returns the key pair loading error.
func serverTLSConfig(t config.TLS, baseDir string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(baseDir, t.CertFile), filepath.Join(baseDir, t.KeyFile))
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"http/1.1"}}, nil
}

// clientTLSConfig mirrors eccom.core.tls.client_ssl_context: with ca_file
// the server chain is verified against it, hostname check off; without it
// the connection is encrypted but unverified (same handshake either way).
// Returns an error when the CA file cannot be read or holds no certificate.
func clientTLSConfig(t config.TLS, baseDir string) (*tls.Config, error) {
	if t.CAFile == "" {
		return &tls.Config{InsecureSkipVerify: true}, nil
	}
	pem, err := os.ReadFile(filepath.Join(baseDir, t.CAFile))
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificate in %s", t.CAFile)
	}
	return &tls.Config{
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			options := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
			for _, cert := range state.PeerCertificates[1:] {
				options.Intermediates.AddCert(cert)
			}
			_, err := state.PeerCertificates[0].Verify(options)
			return err
		},
	}, nil
}
