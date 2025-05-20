// PulseHA - HA Cluster Daemon
// Copyright (C) 2017-2021  Andrew Zak <andrew@linux.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/syleron/pulseha/internal/membership"
	"github.com/syleron/pulseha/internal/server"
	"github.com/syleron/pulseha/packages/config"
	"github.com/syleron/pulseha/packages/logging"
)

var (
	Version string
	Build   string
)

func main() {
	// Check if running in CLI mode
	if len(os.Args) > 1 {
		// Initialize and execute CLI commands
		rootCmd := setupCLI()

		// Initialize flags for quorum commands
		initQuorumFlags(rootCmd)

		if err := rootCmd.Execute(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		return
	}

	// Draw logo
	buildStr := "unknown"
	if len(Build) >= 7 {
		buildStr = Build[0:7]
	}
	fmt.Printf(`
   ___       _                  _
  / _ \_   _| |___  ___  /\  /\/_\
 / /_)/ | | | / __|/ _ \/ /_/ //_\\
/ ___/| |_| | \__ \  __/ __  /  _  \  Version %s
\/     \__,_|_|___/\___\/ /_/\_/ \_/  Build   %s

`, Version, buildStr)

	// Initialize logger
	logger := log.New()
	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	// Set default logging level to debug during development
	logger.SetLevel(log.DebugLevel)

	// Load config
	cfg := config.New()
	if cfg == nil {
		logger.Fatal("Failed to load config")
	}

	// Override logging level from config if specified
	if cfg.Pulse.LoggingLevel != "" {
		if level, err := log.ParseLevel(cfg.Pulse.LoggingLevel); err == nil {
			logger.SetLevel(level)
		} else {
			logger.Warnf("Invalid logging level in config: %s, using debug", cfg.Pulse.LoggingLevel)
		}
	}

	// Setup distributed logging if enabled
	if cfg.Pulse.LogToFile {
		if err := setupLogging(cfg, logger); err != nil {
			logger.Fatal(err)
		}
	}

	// Initialize member list
	memberList := membership.NewMemberList(cfg, logger)

	// Initialize health checker
	healthChecker := membership.NewHealthChecker(memberList, logger)

	// Create and start server
	srv := server.NewServer(cfg, logger, memberList, healthChecker)
	if err := srv.Start(); err != nil {
		logger.Fatalf("Failed to start server: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2)

	// Create a WaitGroup to ensure graceful shutdown
	var wg sync.WaitGroup

	// Add error channel to catch shutdown errors
	errChan := make(chan error, 1)

	for {
		sig := <-sigChan
		switch sig {
		case syscall.SIGUSR2:
			// Reload configuration
			logger.Info("Reloading configuration...")
			if err := cfg.Reload(); err != nil {
				logger.Errorf("Failed to reload config: %v", err)
				continue
			}
			// Restart server with new config
			wg.Add(1)
			go func() {
				defer wg.Done()
				srv.Stop()
				srv = server.NewServer(cfg, logger, memberList, healthChecker)
				if err := srv.Start(); err != nil {
					errChan <- fmt.Errorf("failed to restart server: %v", err)
				}
			}()

		case syscall.SIGINT, syscall.SIGTERM:
			logger.Info("Initiating graceful shutdown...")

			// Stop health checker first
			logger.Debug("Stopping health checker...")
			healthChecker.Stop()

			// Stop server
			logger.Debug("Stopping server...")
			wg.Add(1)
			go func() {
				defer wg.Done()
				srv.Stop()
			}()

			// Wait for all components to shut down
			logger.Debug("Waiting for all components to shut down...")
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			// Wait for shutdown with timeout
			select {
			case <-done:
				logger.Info("Graceful shutdown completed")
				os.Exit(0)
			case err := <-errChan:
				logger.Errorf("Error during shutdown: %v", err)
				os.Exit(1)
			case <-time.After(10 * time.Second):
				logger.Error("Shutdown timed out, forcing exit")
				os.Exit(1)
			}
		}
	}
}

// initQuorumFlags initializes flags for quorum commands
func initQuorumFlags(rootCmd *cobra.Command) {
	// Find quorum command
	var quorumCmd *cobra.Command
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "quorum" {
			quorumCmd = cmd
			break
		}
	}

	if quorumCmd == nil {
		return
	}

	// Find and initialize list-sessions command flags
	for _, cmd := range quorumCmd.Commands() {
		if cmd.Name() == "list-sessions" {
			cmd.Flags().Bool("include-completed", false, "Include completed voting sessions")
		} else if cmd.Name() == "config" {
			cmd.Flags().Int("min-nodes", 2, "Minimum number of nodes required for quorum")
			cmd.Flags().Bool("majority-mode", false, "Use majority of nodes for quorum instead of fixed minimum")
		}
	}
}

func setupLogging(cfg *config.Config, logger *log.Logger) error {
	// Setup file logging
	// Restrict permissions so that only the owner can modify the log and
	// the group can read it. This avoids exposing potentially sensitive
	// log output to the world.
	logFile, err := os.OpenFile(cfg.Pulse.LogFileLocation, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}

	// Create distributed logger
	pulseLogger, err := logging.NewLogger(nil) // We'll need to implement broadcast later
	if err != nil {
		return fmt.Errorf("failed to create distributed logger: %v", err)
	}

	// Add hooks
	logger.AddHook(pulseLogger)

	// Create a multi-writer for both file and stdout
	mw := io.MultiWriter(os.Stdout, logFile)
	logger.SetOutput(mw)

	return nil
}
