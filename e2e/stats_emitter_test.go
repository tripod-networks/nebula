//go:build e2e_testing

package e2e

import (
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/slackhq/nebula/cert"
	"github.com/slackhq/nebula/cert_test"
	"github.com/stretchr/testify/assert"
)

// With no stats.type, Main wires the emitter so its gauges exist as soon as Start returns
func TestStatsEmitterRunsWithNoExporter(t *testing.T) {
	ca, _, caKey, _ := cert_test.NewTestCaCert(cert.Version1, cert.Curve_CURVE25519, time.Now(), time.Now().Add(10*time.Minute), nil, nil, []string{})
	control, _, _, _ := newSimpleServer(cert.Version1, ca, caKey, "me", "10.128.0.1/24", nil)
	t.Cleanup(control.Stop)

	metrics.DefaultRegistry.Unregister("hostmap.main.hosts")
	control.Start()

	assert.NotNil(t, metrics.DefaultRegistry.Get("hostmap.main.hosts"))
}
