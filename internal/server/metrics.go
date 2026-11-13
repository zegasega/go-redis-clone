package server

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	connectedClients atomic.Int64
	totalCommands    atomic.Uint64
	expiredKeys      atomic.Uint64
	startTime        time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

func (m *Metrics) ClientConnected() {
	m.connectedClients.Add(1)
}

func (m *Metrics) ClientDisconnected() {
	m.connectedClients.Add(-1)
}

func (m *Metrics) CommandExecuted() {
	m.totalCommands.Add(1)
}

func (m *Metrics) KeysExpired(count uint64) {
	m.expiredKeys.Add(count)
}

func (m *Metrics) Uptime() time.Duration {
	return time.Since(m.startTime)
}
