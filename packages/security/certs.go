package security

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// GenerateCertificates generates a CA and node certificates for mTLS
func GenerateCertificates(hostname string) error {
	// Create cert directory if it doesn't exist
	if err := os.MkdirAll(CertDir, 0755); err != nil {
		return fmt.Errorf("failed to create cert directory: %v", err)
	}

	// Generate CA private key
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate CA private key: %v", err)
	}

	// Create CA certificate
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"PulseHA"},
			CommonName:   "PulseHA CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0), // 10 years validity
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// Create CA certificate
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create CA certificate: %v", err)
	}

	// Save CA certificate
	caCertFile, err := os.Create(filepath.Join(CertDir, "ca.crt"))
	if err != nil {
		return fmt.Errorf("failed to create CA certificate file: %v", err)
	}
	pem.Encode(caCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	caCertFile.Close()

	// Save CA private key
	caKeyFile, err := os.Create(filepath.Join(CertDir, "ca.key"))
	if err != nil {
		return fmt.Errorf("failed to create CA key file: %v", err)
	}
	pem.Encode(caKeyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caKey)})
	caKeyFile.Close()

	// Generate node key pair
	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate node private key: %v", err)
	}

	// Create node certificate
	nodeCert := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"PulseHA"},
			CommonName:   hostname,
		},
		DNSNames:     []string{hostname, "localhost"},
		IPAddresses:  nil, // Add IPs if needed
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(1, 0, 0), // 1 year validity
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// Create node certificate
	nodeCertBytes, err := x509.CreateCertificate(rand.Reader, nodeCert, ca, &nodeKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create node certificate: %v", err)
	}

	// Save node certificate
	nodeCertFile, err := os.Create(filepath.Join(CertDir, "pulseha.crt"))
	if err != nil {
		return fmt.Errorf("failed to create node certificate file: %v", err)
	}
	pem.Encode(nodeCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: nodeCertBytes})
	nodeCertFile.Close()

	// Save node private key
	nodeKeyFile, err := os.Create(filepath.Join(CertDir, "pulseha.key"))
	if err != nil {
		return fmt.Errorf("failed to create node key file: %v", err)
	}
	pem.Encode(nodeKeyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(nodeKey)})
	nodeKeyFile.Close()

	return nil
}
