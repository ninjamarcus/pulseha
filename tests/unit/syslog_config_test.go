package unit

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syleron/pulseha/packages/config"
)

func TestSyslogConfigDefaults(t *testing.T) {
	// Set test environment
	os.Setenv("PULSEHA_TEST", "true")
	defer os.Unsetenv("PULSEHA_TEST")

	cfg, err := config.New()
	assert.NoError(t, err, "New should not return error in test mode")

	assert.True(t, cfg.Pulse.LogToSyslog, "Syslog should be enabled by default")
	assert.Equal(t, "", cfg.Pulse.SyslogNetwork, "Default syslog network should be empty (local)")
	assert.Equal(t, "", cfg.Pulse.SyslogAddress, "Default syslog address should be empty (local)")
	assert.Equal(t, "LOG_INFO", cfg.Pulse.SyslogFacility, "Default syslog facility should be LOG_INFO")
	assert.Equal(t, "pulseha", cfg.Pulse.SyslogTag, "Default syslog tag should be 'pulseha'")
}

func TestSyslogConfigMigration(t *testing.T) {
	// Set test environment
	os.Setenv("PULSEHA_TEST", "true")
	defer os.Unsetenv("PULSEHA_TEST")

	// Create old config without syslog fields
	oldConfigJSON := `{
		"pulseha": {
			"hcs_interval": 1000,
			"fos_interval": 5000,
			"fo_limit": 10000,
			"local_node": "test-node",
			"logging_level": "info",
			"auto_failback": true,
			"log_to_file": true,
			"log_file_location": "/tmp/test.log",
			"mode": "active-passive"
		},
		"floating_ip_groups": {},
		"nodes": {},
		"plugins": {}
	}`

	var cfg config.Config
	err := json.Unmarshal([]byte(oldConfigJSON), &cfg)
	assert.NoError(t, err, "Should unmarshal old config format")

	// Syslog fields should be empty (zero values)
	assert.False(t, cfg.Pulse.LogToSyslog, "Old config should have false for LogToSyslog")
	assert.Equal(t, "", cfg.Pulse.SyslogTag, "Old config should have empty SyslogTag")
	assert.Equal(t, "", cfg.Pulse.SyslogFacility, "Old config should have empty SyslogFacility")
}

func TestSyslogConfigValidation(t *testing.T) {
	// Set test environment
	os.Setenv("PULSEHA_TEST", "true")
	defer os.Unsetenv("PULSEHA_TEST")

	testCases := []struct {
		name     string
		config   config.Local
		expectOK bool
	}{
		{
			name: "Valid syslog config with local syslog",
			config: config.Local{
				HealthCheckInterval: 1000,
				FailOverInterval:    5000,
				FailOverLimit:       10000,
				LogToSyslog:         true,
				SyslogNetwork:       "",
				SyslogAddress:       "",
				SyslogFacility:      "LOG_LOCAL0",
				SyslogTag:           "pulseha",
				Mode:                "active-passive",
			},
			expectOK: true,
		},
		{
			name: "Valid syslog config with remote syslog",
			config: config.Local{
				HealthCheckInterval: 1000,
				FailOverInterval:    5000,
				FailOverLimit:       10000,
				LogToSyslog:         true,
				SyslogNetwork:       "udp",
				SyslogAddress:       "192.168.1.100:514",
				SyslogFacility:      "LOG_LOCAL1",
				SyslogTag:           "pulseha-node1",
				Mode:                "active-passive",
			},
			expectOK: true,
		},
		{
			name: "Valid config with syslog disabled",
			config: config.Local{
				HealthCheckInterval: 1000,
				FailOverInterval:    5000,
				FailOverLimit:       10000,
				LogToSyslog:         false,
				SyslogNetwork:       "",
				SyslogAddress:       "",
				SyslogFacility:      "",
				SyslogTag:           "",
				Mode:                "active-passive",
			},
			expectOK: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				Pulse:   tc.config,
				Groups:  make(map[string][]string),
				Nodes:   make(map[string]*config.Node),
				Plugins: make(map[string]interface{}),
			}

			err := cfg.Validate()
			if tc.expectOK {
				assert.NoError(t, err, "Config validation should pass for: %s", tc.name)
			} else {
				assert.Error(t, err, "Config validation should fail for: %s", tc.name)
			}
		})
	}
}

func TestSyslogConfigSerialization(t *testing.T) {
	// Set test environment
	os.Setenv("PULSEHA_TEST", "true")
	defer os.Unsetenv("PULSEHA_TEST")

	// Create config with syslog settings
	cfg := &config.Config{
		Pulse: config.Local{
			HealthCheckInterval: 1000,
			FailOverInterval:    5000,
			FailOverLimit:       10000,
			LoggingLevel:        "info",
			AutoFailback:        true,
			LogToFile:           true,
			LogFileLocation:     "/tmp/test.log",
			LogToSyslog:         true,
			SyslogNetwork:       "udp",
			SyslogAddress:       "syslog.example.com:514",
			SyslogFacility:      "LOG_LOCAL2",
			SyslogTag:           "pulseha-test",
			Mode:                "active-passive",
		},
		Groups:  make(map[string][]string),
		Nodes:   make(map[string]*config.Node),
		Plugins: make(map[string]interface{}),
	}

	// Serialize to JSON
	data, err := json.MarshalIndent(cfg, "", "    ")
	assert.NoError(t, err, "Should serialize config to JSON")

	// Deserialize back
	var cfg2 config.Config
	err = json.Unmarshal(data, &cfg2)
	assert.NoError(t, err, "Should deserialize config from JSON")

	// Verify syslog fields are preserved
	assert.Equal(t, cfg.Pulse.LogToSyslog, cfg2.Pulse.LogToSyslog, "LogToSyslog should be preserved")
	assert.Equal(t, cfg.Pulse.SyslogNetwork, cfg2.Pulse.SyslogNetwork, "SyslogNetwork should be preserved")
	assert.Equal(t, cfg.Pulse.SyslogAddress, cfg2.Pulse.SyslogAddress, "SyslogAddress should be preserved")
	assert.Equal(t, cfg.Pulse.SyslogFacility, cfg2.Pulse.SyslogFacility, "SyslogFacility should be preserved")
	assert.Equal(t, cfg.Pulse.SyslogTag, cfg2.Pulse.SyslogTag, "SyslogTag should be preserved")
}
