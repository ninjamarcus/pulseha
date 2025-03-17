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

package logging

import (
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/syleron/pulseha/rpc"
)

type Logging struct {
	sync.Mutex
	Broadcast func(*rpc.LogsRequest) error
}

// NewLogger returns a new distributes logging object
func NewLogger(broadcast func(*rpc.LogsRequest) error) (*Logging, error) {
	return &Logging{
		Broadcast: broadcast,
	}, nil
}

// Fire implements the logrus.Hook interface
func (l *Logging) Fire(entry *logrus.Entry) error {
	l.Lock()
	defer l.Unlock()

	if l.Broadcast == nil {
		return nil
	}

	// Convert logrus level to rpc LogLevel
	var level rpc.LogLevel
	switch entry.Level {
	case logrus.DebugLevel:
		level = rpc.LogLevel_DEBUG
	case logrus.InfoLevel:
		level = rpc.LogLevel_INFO
	case logrus.WarnLevel:
		level = rpc.LogLevel_WARNING
	case logrus.ErrorLevel:
		level = rpc.LogLevel_ERROR
	default:
		level = rpc.LogLevel_INFO
	}

	// Create log request
	req := &rpc.LogsRequest{
		Level:   level,
		Message: entry.Message,
		Node:    entry.Data["node"].(string),
	}

	return l.Broadcast(req)
}

// Levels implements the logrus.Hook interface
func (l *Logging) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.DebugLevel,
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.ErrorLevel,
	}
}

// Debug logs a debug message
func (l *Logging) Debug(msg string) {
	if l.Broadcast != nil {
		l.Broadcast(&rpc.LogsRequest{
			Level:   rpc.LogLevel_DEBUG,
			Message: msg,
		})
	}
}

// Info logs an info message
func (l *Logging) Info(msg string) {
	if l.Broadcast != nil {
		l.Broadcast(&rpc.LogsRequest{
			Level:   rpc.LogLevel_INFO,
			Message: msg,
		})
	}
}

// Warning logs a warning message
func (l *Logging) Warning(msg string) {
	if l.Broadcast != nil {
		l.Broadcast(&rpc.LogsRequest{
			Level:   rpc.LogLevel_WARNING,
			Message: msg,
		})
	}
}

// Error logs an error message
func (l *Logging) Error(msg string) {
	if l.Broadcast != nil {
		l.Broadcast(&rpc.LogsRequest{
			Level:   rpc.LogLevel_ERROR,
			Message: msg,
		})
	}
}
