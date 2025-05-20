package unit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syleron/pulseha/packages/security"
)

func TestGenerateCertificatesCreatesKeyWith0600Mode(t *testing.T) {
	dir := t.TempDir()
	orig := security.CertDir
	security.CertDir = dir
	defer func() { security.CertDir = orig }()

	err := security.GenerateCertificates("localhost")
	assert.NoError(t, err)

	info, err := os.Stat(filepath.Join(dir, "pulseha.key"))
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
}
