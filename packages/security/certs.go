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
	caCertPath := filepath.Join(CertDir, "ca.crt")
	caCertTmpPath := caCertPath + ".tmp"
	caCertFile, err := os.OpenFile(caCertTmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create CA certificate file: %v", err)
	}
	if err := pem.Encode(caCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes}); err != nil {
		caCertFile.Close()
		os.Remove(caCertTmpPath)
		return fmt.Errorf("failed to write CA certificate: %v", err)
	}
	if err := caCertFile.Close(); err != nil {
		os.Remove(caCertTmpPath)
		return fmt.Errorf("failed to close CA certificate file: %v", err)
	}
	if err := os.Rename(caCertTmpPath, caCertPath); err != nil {
		os.Remove(caCertTmpPath)
		return fmt.Errorf("failed to atomically create CA certificate file: %v", err)
	}

	// Save CA private key
	caKeyPath := filepath.Join(CertDir, "ca.key")
	caKeyTmpPath := caKeyPath + ".tmp"
	caKeyFile, err := os.OpenFile(caKeyTmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create CA key file: %v", err)
	}
	if err := pem.Encode(caKeyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caKey)}); err != nil {
		caKeyFile.Close()
		os.Remove(caKeyTmpPath)
		return fmt.Errorf("failed to write CA key: %v", err)
	}
	if err := caKeyFile.Close(); err != nil {
		os.Remove(caKeyTmpPath)
		return fmt.Errorf("failed to close CA key file: %v", err)
	}
	if err := os.Rename(caKeyTmpPath, caKeyPath); err != nil {
		os.Remove(caKeyTmpPath)
		return fmt.Errorf("failed to atomically create CA key file: %v", err)
	}

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
	nodeCertPath := filepath.Join(CertDir, "pulseha.crt")
	nodeCertTmpPath := nodeCertPath + ".tmp"
	nodeCertFile, err := os.OpenFile(nodeCertTmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create node certificate file: %v", err)
	}
	if err := pem.Encode(nodeCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: nodeCertBytes}); err != nil {
		nodeCertFile.Close()
		os.Remove(nodeCertTmpPath)
		return fmt.Errorf("failed to write node certificate: %v", err)
	}
	if err := nodeCertFile.Close(); err != nil {
		os.Remove(nodeCertTmpPath)
		return fmt.Errorf("failed to close node certificate file: %v", err)
	}
	if err := os.Rename(nodeCertTmpPath, nodeCertPath); err != nil {
		os.Remove(nodeCertTmpPath)
		return fmt.Errorf("failed to atomically create node certificate file: %v", err)
	}

	// Save node private key
	nodeKeyPath := filepath.Join(CertDir, "pulseha.key")
	nodeKeyTmpPath := nodeKeyPath + ".tmp"
	nodeKeyFile, err := os.OpenFile(nodeKeyTmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create node key file: %v", err)
	}
	if err := pem.Encode(nodeKeyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(nodeKey)}); err != nil {
		nodeKeyFile.Close()
		os.Remove(nodeKeyTmpPath)
		return fmt.Errorf("failed to write node key: %v", err)
	}
	if err := nodeKeyFile.Close(); err != nil {
		os.Remove(nodeKeyTmpPath)
		return fmt.Errorf("failed to close node key file: %v", err)
	}
	if err := os.Rename(nodeKeyTmpPath, nodeKeyPath); err != nil {
		os.Remove(nodeKeyTmpPath)
		return fmt.Errorf("failed to atomically create node key file: %v", err)
	}

	return nil
}
