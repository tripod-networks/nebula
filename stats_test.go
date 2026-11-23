package nebula

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/slackhq/nebula/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStatsServer(t *testing.T) (*statsServer, *config.C) {
	t.Helper()
	l := slog.New(slog.DiscardHandler)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &statsServer{
		l:   l,
		ctx: ctx,
	}, config.NewC(l)
}

func setStatsConfig(c *config.C, m map[string]any) {
	c.Settings["stats"] = m
}

func currentRuntime(s *statsServer) *statsRuntime {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	return s.run
}

func TestStatsServer_reload_initial_disabled(t *testing.T) {
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{"type": "none"})

	require.NoError(t, s.reload(c, true))
	assert.False(t, s.enabled.Load())
	assert.Nil(t, currentRuntime(s))
}

func TestStatsServer_reload_initial_invalidInterval(t *testing.T) {
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":   "graphite",
		"host":   "127.0.0.1:0",
		"prefix": "test",
	})

	err := s.reload(c, true)
	require.Error(t, err)
	assert.False(t, s.enabled.Load())
}

func TestStatsServer_reload_initial_unknownType(t *testing.T) {
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "carbon",
		"interval": "1s",
	})

	err := s.reload(c, true)
	require.Error(t, err)
	assert.False(t, s.enabled.Load())
}

func TestStatsServer_reload_unchanged_noOp(t *testing.T) {
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{"type": "none"})

	require.NoError(t, s.reload(c, true))
	require.NoError(t, s.reload(c, false))
	assert.False(t, s.enabled.Load())
}

func TestStatsServer_reload_initial_graphite(t *testing.T) {
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "graphite",
		"interval": "1s",
		"protocol": "tcp",
		"host":     "127.0.0.1:2003",
		"prefix":   "test",
	})

	require.NoError(t, s.reload(c, true))
	assert.True(t, s.enabled.Load())
	// reload only records config; Start builds the runtime.
	assert.Nil(t, currentRuntime(s))
}

func TestStatsServer_reload_initial_prometheus(t *testing.T) {
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:0",
		"path":     "/metrics",
	})

	require.NoError(t, s.reload(c, true))
	assert.True(t, s.enabled.Load())
	// reload only records config; Start builds the runtime.
	assert.Nil(t, currentRuntime(s))
}

func TestStatsServer_Start_graphite_blocksUntilStop(t *testing.T) {
	h := newGraphiteHost(t, 0)

	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "graphite",
		"interval": "1s",
		"protocol": "tcp",
		"host":     h.ln.Addr().String(),
		"prefix":   "test",
	})
	require.NoError(t, s.reload(c, true))

	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()

	// Wait for Start to publish runtime state.
	waitFor(t, func() bool { return currentRuntime(s) != nil })
	rt := currentRuntime(s)
	require.NotNil(t, rt)
	assert.Nil(t, rt.listener, "graphite has no listener")

	s.Stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("graphite Start did not return after Stop")
	}
	assert.Nil(t, currentRuntime(s))
}

func TestStatsServer_StartStop_lifecycle(t *testing.T) {
	port := freeTCPPort(t)
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:" + port,
		"path":     "/metrics",
	})
	require.NoError(t, s.reload(c, true))

	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()

	waitForListening(t, "127.0.0.1:"+port)
	rt := currentRuntime(s)
	require.NotNil(t, rt)
	require.NotNil(t, rt.listener)

	s.Stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
	assert.Nil(t, currentRuntime(s))
}

func TestStatsServer_reload_disable_stopsRunningRuntime(t *testing.T) {
	port := freeTCPPort(t)
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:" + port,
		"path":     "/metrics",
	})
	require.NoError(t, s.reload(c, true))

	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()
	waitForListening(t, "127.0.0.1:"+port)

	setStatsConfig(c, map[string]any{"type": "none"})
	require.NoError(t, s.reload(c, false))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after reload disabled stats")
	}
	assert.False(t, s.enabled.Load())
	assert.Nil(t, currentRuntime(s))
}

func TestStatsServer_reload_changeListener_restartsListener(t *testing.T) {
	port1 := freeTCPPort(t)
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:" + port1,
		"path":     "/metrics",
	})
	require.NoError(t, s.reload(c, true))

	firstDone := make(chan struct{})
	go func() {
		s.Start()
		close(firstDone)
	}()
	waitForListening(t, "127.0.0.1:"+port1)
	first := currentRuntime(s)
	require.NotNil(t, first)

	port2 := freeTCPPort(t)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:" + port2,
		"path":     "/metrics",
	})
	require.NoError(t, s.reload(c, false))

	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old Start did not return after reload")
	}

	waitForListening(t, "127.0.0.1:"+port2)
	second := currentRuntime(s)
	require.NotNil(t, second)
	assert.NotSame(t, first, second, "expected a new runtime after listen address change")

	s.Stop()
}

func TestStatsServer_Stop_beforeStart_doesNotBlock(t *testing.T) {
	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:0",
		"path":     "/metrics",
	})
	require.NoError(t, s.reload(c, true))

	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop hung with no runtime started")
	}
}

func TestStatsServer_configTest_validatesWithoutSpawning(t *testing.T) {
	s, c := newTestStatsServer(t)
	s.configTest = true
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:0",
		"path":     "/metrics",
	})

	require.NoError(t, s.reload(c, true))
	s.Start()
	assert.Nil(t, currentRuntime(s))
}

func TestStatsServer_ctxCancel_unblocksStart(t *testing.T) {
	// Ensures ctx cancellation alone (no explicit Stop) tears down both
	// graphite and prom Start invocations.
	port := freeTCPPort(t)
	l := slog.New(slog.DiscardHandler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &statsServer{l: l, ctx: ctx}
	c := config.NewC(l)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:" + port,
		"path":     "/metrics",
	})
	require.NoError(t, s.reload(c, true))

	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()
	waitForListening(t, "127.0.0.1:"+port)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after ctx cancel")
	}
}

func TestStatsServer_listenerBindFailure_sameCfgReloadRetries(t *testing.T) {
	// Hold the port so ListenAndServe will fail on first Start.
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := strconv.Itoa(blocker.Addr().(*net.TCPAddr).Port)

	s, c := newTestStatsServer(t)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:" + port,
		"path":     "/metrics",
	})
	require.NoError(t, s.reload(c, true))

	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after bind failure")
	}
	// Bind failure should have dropped the cached config so a same-cfg
	// SIGHUP can retry.
	s.runMu.Lock()
	cfgAfterFailure := s.runCfg
	s.runMu.Unlock()
	assert.Nil(t, cfgAfterFailure)

	// Free the port and reload with the same config; Start should fire again.
	require.NoError(t, blocker.Close())
	require.NoError(t, s.reload(c, false))

	waitForListening(t, "127.0.0.1:"+port)
	require.NotNil(t, currentRuntime(s))

	s.Stop()
}

func waitForListening(t *testing.T, addr string) {
	t.Helper()
	waitFor(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	})
}

// stopAtCleanup returns the ctx to build *s with, and stops it when the test ends. Cancelling first turns away a Start
// a reload spawned that hasn't run yet, then Stop waits out the last pass, so nothing captures into the next test
func stopAtCleanup(t *testing.T, s **statsServer) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		if *s != nil {
			(*s).Stop()
		}
	})
	return ctx
}

func freeTCPPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return strconv.Itoa(port)
}

// Each export has to see what emitStats set in the same pass
func TestStatsServer_emitStatsRunsBeforeExport(t *testing.T) {
	defer metrics.DefaultRegistry.Unregister("emitter.order.test")

	l := slog.New(slog.DiscardHandler)
	c := config.NewC(l)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1s",
		"listen":   "127.0.0.1:0",
		"path":     "/metrics",
	})
	gauge := metrics.GetOrRegisterGauge("emitter.order.test", nil)
	s, err := newStatsServerFromConfig(t.Context(), l, c, "", false, func() { gauge.Update(42) })
	require.NoError(t, err)

	s.runMu.Lock()
	fns, srv := s.buildRuntime(t.Context(), *s.runCfg)
	s.runMu.Unlock()
	require.NotNil(t, srv)
	for _, fn := range fns {
		fn()
	}

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	assert.Contains(t, rec.Body.String(), "emitter_order_test 42")
}

// A scrape that lands the moment the listener is up already sees what emitStats set (issue #907)
func TestStatsServer_Start_primesBeforeServing(t *testing.T) {
	defer metrics.DefaultRegistry.Unregister("prime.before.serving")

	port := freeTCPPort(t)
	l := slog.New(slog.DiscardHandler)
	c := config.NewC(l)
	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1h",
		"listen":   "127.0.0.1:" + port,
		"path":     "/metrics",
	})
	gauge := metrics.GetOrRegisterGauge("prime.before.serving", nil)
	// The pause would let a listener that came up before the prime serve the zero value
	var s *statsServer
	ctx := stopAtCleanup(t, &s)
	s, err := newStatsServerFromConfig(ctx, l, c, "", false, func() {
		time.Sleep(200 * time.Millisecond)
		gauge.Update(42)
	})
	require.NoError(t, err)

	go s.Start()
	waitForListening(t, "127.0.0.1:"+port)

	resp, err := http.Get("http://127.0.0.1:" + port + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "prime_before_serving 42")
}

// Start captures once before the first tick, the interval here is an hour
func TestStatsServer_Start_primes(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	l := slog.New(slog.DiscardHandler)
	c := config.NewC(l)
	setStatsConfig(c, map[string]any{"type": "graphite", "interval": "1h", "host": ln.Addr().String()})
	primed := make(chan struct{}, 1)
	var s *statsServer
	ctx := stopAtCleanup(t, &s)
	s, err = newStatsServerFromConfig(ctx, l, c, "", false, func() {
		select {
		case primed <- struct{}{}:
		default:
		}
	})
	require.NoError(t, err)

	go s.Start()
	select {
	case <-primed:
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was captured before the first tick")
	}
}

// Graphite has to ship what emitStats set in the same pass, not the pass before
func TestStatsServer_emitStatsRunsBeforeGraphiteExport(t *testing.T) {
	defer metrics.DefaultRegistry.Unregister("emitter.graphite.test")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	received := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		b, _ := io.ReadAll(conn)
		received <- string(b)
	}()

	l := slog.New(slog.DiscardHandler)
	c := config.NewC(l)
	setStatsConfig(c, map[string]any{"type": "graphite", "interval": "1h", "host": ln.Addr().String()})
	gauge := metrics.GetOrRegisterGauge("emitter.graphite.test", nil)
	s, err := newStatsServerFromConfig(t.Context(), l, c, "", false, func() { gauge.Update(42) })
	require.NoError(t, err)

	s.runMu.Lock()
	fns, _ := s.buildRuntime(t.Context(), *s.runCfg)
	s.runMu.Unlock()
	for _, fn := range fns {
		fn()
	}

	select {
	case body := <-received:
		assert.Contains(t, body, ".emitter.graphite.test.value 42 ")
	case <-time.After(5 * time.Second):
		t.Fatal("graphite sent nothing")
	}
}

// stats.interval reloads, and the runtime a reload starts still has emitStats
func TestStatsServer_reloadMovesTheInterval(t *testing.T) {
	var passes atomic.Int64
	l := slog.New(slog.DiscardHandler)
	c := config.NewC(l)
	// Nothing listens here, each export fails at once
	host := "127.0.0.1:" + freeTCPPort(t)
	setStatsConfig(c, map[string]any{"type": "graphite", "interval": "1h", "host": host})
	var s *statsServer
	ctx := stopAtCleanup(t, &s)
	s, err := newStatsServerFromConfig(ctx, l, c, "", false, func() { passes.Add(1) })
	require.NoError(t, err)
	go s.Start()
	waitFor(t, func() bool { return passes.Load() == 1 })

	setStatsConfig(c, map[string]any{"type": "graphite", "interval": "20ms", "host": host})
	require.NoError(t, s.reload(c, false))
	waitFor(t, func() bool { return passes.Load() >= 6 })

	setStatsConfig(c, map[string]any{"type": "graphite", "interval": "1h", "host": host})
	require.NoError(t, s.reload(c, false))
	// The reload's Stop waited out the 20ms loop. The new runtime's prime may land either side of this read, it is the
	// only pass left to come
	stopped := passes.Load()
	time.Sleep(300 * time.Millisecond)
	assert.LessOrEqual(t, passes.Load()-stopped, int64(1), "the 20ms loop kept running after the reload")
}

// A reload's new runtime never captures while the old one is still in a pass, go-metrics keeps its GC capture state
// in unguarded globals
func TestStatsServer_reloadWaitsForTheLastPass(t *testing.T) {
	var active, most atomic.Int64
	l := slog.New(slog.DiscardHandler)
	c := config.NewC(l)
	host := "127.0.0.1:" + freeTCPPort(t)
	setStatsConfig(c, map[string]any{"type": "graphite", "interval": "10ms", "host": host})
	var s *statsServer
	ctx := stopAtCleanup(t, &s)
	s, err := newStatsServerFromConfig(ctx, l, c, "", false, func() {
		n := active.Add(1)
		for {
			m := most.Load()
			if n <= m || most.CompareAndSwap(m, n) {
				break
			}
		}
		// Long enough that a reload almost always lands mid pass
		time.Sleep(50 * time.Millisecond)
		active.Add(-1)
	})
	require.NoError(t, err)
	go s.Start()

	for i := range 6 {
		setStatsConfig(c, map[string]any{"type": "graphite", "interval": fmt.Sprintf("%dms", 11-i%2), "host": host})
		require.NoError(t, s.reload(c, false))
		time.Sleep(30 * time.Millisecond)
	}
	assert.Equal(t, int64(1), most.Load())
}

// Nothing captures once Stop returns, a pass in progress is waited out
func TestStatsServer_Stop_waitsForThePass(t *testing.T) {
	var inPass atomic.Bool
	entered := make(chan struct{}, 1)
	l := slog.New(slog.DiscardHandler)
	c := config.NewC(l)
	setStatsConfig(c, map[string]any{"type": "graphite", "interval": "1h", "host": "127.0.0.1:" + freeTCPPort(t)})
	var s *statsServer
	ctx := stopAtCleanup(t, &s)
	s, err := newStatsServerFromConfig(ctx, l, c, "", false, func() {
		inPass.Store(true)
		select {
		case entered <- struct{}{}:
		default:
		}
		time.Sleep(100 * time.Millisecond)
		inPass.Store(false)
	})
	require.NoError(t, err)

	go s.Start()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("never captured")
	}
	s.Stop()
	assert.False(t, inPass.Load(), "Stop returned with a pass still running")
}

// A Start that arrives while Stop waits out the last pass doesn't capture alongside it
func TestStatsServer_Start_duringStopWaitsForThePass(t *testing.T) {
	var active, most atomic.Int64
	entered := make(chan struct{}, 1)
	l := slog.New(slog.DiscardHandler)
	c := config.NewC(l)
	setStatsConfig(c, map[string]any{"type": "graphite", "interval": "1h", "host": "127.0.0.1:" + freeTCPPort(t)})
	var s *statsServer
	ctx := stopAtCleanup(t, &s)
	s, err := newStatsServerFromConfig(ctx, l, c, "", false, func() {
		n := active.Add(1)
		for {
			m := most.Load()
			if n <= m || most.CompareAndSwap(m, n) {
				break
			}
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		time.Sleep(200 * time.Millisecond)
		active.Add(-1)
	})
	require.NoError(t, err)

	go s.Start()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("never captured")
	}
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	// Stop holds runMu for its whole wait, the next Start has to arrive after it took it
	waitFor(t, func() bool {
		if s.runMu.TryLock() {
			s.runMu.Unlock()
			return false
		}
		return true
	})
	go s.Start()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
	waitFor(t, func() bool { return len(entered) == 1 })
	assert.Equal(t, int64(1), most.Load(), "a Start captured alongside the pass Stop was waiting out")
}

func newEmitterOnlyServer(t *testing.T, c *config.C, configTest bool, passes *atomic.Int64) *statsServer {
	t.Helper()
	var s *statsServer
	ctx := stopAtCleanup(t, &s)
	s, err := newStatsServerFromConfig(ctx, slog.New(slog.DiscardHandler), c, "", configTest, func() { passes.Add(1) })
	require.NoError(t, err)
	return s
}

func currentLoopDone(s *statsServer) chan struct{} {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	return s.loopDone
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// With no exporter the emitter still runs, once before startEmitter returns and then on every tick
func TestStatsServer_startEmitter_noExporter(t *testing.T) {
	var passes atomic.Int64
	c := config.NewC(slog.New(slog.DiscardHandler))
	setStatsConfig(c, map[string]any{"interval": "20ms"})
	s := newEmitterOnlyServer(t, c, false, &passes)

	s.startEmitter()
	assert.Equal(t, int64(1), passes.Load(), "the first emit has to land before startEmitter returns")
	assert.Nil(t, currentRuntime(s).listener)

	waitFor(t, func() bool { return passes.Load() >= 3 })

	s.Stop()
	assert.True(t, isClosed(currentLoopDone(s)), "Stop returned with the loop still running")
	assert.Nil(t, currentRuntime(s))
}

// An unset stats.interval is not an error with no exporter, the emitter takes the 10s default
func TestStatsServer_startEmitter_defaultInterval(t *testing.T) {
	var passes atomic.Int64
	c := config.NewC(slog.New(slog.DiscardHandler))
	s := newEmitterOnlyServer(t, c, false, &passes)

	assert.Equal(t, 10*time.Second, s.runCfg.interval)
	s.startEmitter()
	assert.Equal(t, int64(1), passes.Load())
}

// The emitter alone doesn't register the runtime or debug GC sets, those stay with the exporters.
// Registration is once per process, so the check runs in a child no other test has run in.
func TestStatsServer_startEmitter_registersNoRuntimeMetrics(t *testing.T) {
	if os.Getenv("NEBULA_STATS_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run", "^"+regexp.QuoteMeta(t.Name())+"$", "-test.v", "-test.count=1", "-test.timeout=60s")
		cmd.Env = append(os.Environ(), "NEBULA_STATS_CHILD=1")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		require.Contains(t, string(out), "--- PASS: "+t.Name(), "the child ran no such test")
		return
	}

	var passes atomic.Int64
	c := config.NewC(slog.New(slog.DiscardHandler))
	s := newEmitterOnlyServer(t, c, false, &passes)
	s.startEmitter()

	require.Equal(t, int64(1), passes.Load())
	assert.Nil(t, metrics.DefaultRegistry.Get("runtime.MemStats.Alloc"))
	assert.Nil(t, metrics.DefaultRegistry.Get("debug.GCStats.LastGC"))
}

// A non-positive or unparseable interval is no error with no exporter, it falls back to 10s
func TestStatsServer_noExporter_intervalFallback(t *testing.T) {
	for _, interval := range []string{"0s", "-1s", "not-a-duration"} {
		t.Run(interval, func(t *testing.T) {
			c := config.NewC(slog.New(slog.DiscardHandler))
			setStatsConfig(c, map[string]any{"interval": interval})

			cfg, err := loadStatsConfig(c)
			require.NoError(t, err)
			assert.Equal(t, 10*time.Second, cfg.interval)
		})
	}
}

// A Start spawned for an exporter config can run after a reload to no exporter, it builds nothing
func TestStatsServer_Start_staleForNoExporter(t *testing.T) {
	var passes atomic.Int64
	c := config.NewC(slog.New(slog.DiscardHandler))
	s := newEmitterOnlyServer(t, c, false, &passes)
	s.enabled.Store(true)

	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start built a runtime for a config with no exporter")
	}
	assert.Nil(t, currentRuntime(s))

	s.enabled.Store(false)
	s.startEmitter()
	assert.Equal(t, int64(1), passes.Load())
	assert.NotNil(t, currentRuntime(s))
}

// The first emit holds the runtime slot, so a caller that finds it taken returns after the emit
func TestStatsServer_startEmitter_firstEmitHoldsTheSlot(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c := config.NewC(slog.New(slog.DiscardHandler))
	var s *statsServer
	ctx := stopAtCleanup(t, &s)
	s, err := newStatsServerFromConfig(ctx, slog.New(slog.DiscardHandler), c, "", false, func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		s.startEmitter()
		close(done)
	}()
	<-entered
	if s.runMu.TryLock() {
		s.runMu.Unlock()
		t.Error("the first emit ran after the runtime slot was released")
	}
	close(release)
	<-done
}

// A config test builds the stats server and nothing runs
func TestStatsServer_startEmitter_configTest(t *testing.T) {
	var passes atomic.Int64
	c := config.NewC(slog.New(slog.DiscardHandler))
	setStatsConfig(c, map[string]any{"interval": "20ms"})
	s := newEmitterOnlyServer(t, c, true, &passes)

	s.startEmitter()
	s.Start()
	assert.Equal(t, int64(0), passes.Load())
	assert.Nil(t, currentRuntime(s))
	assert.Nil(t, currentLoopDone(s))
}

// A reload that changes only the interval moves the loop to it, the 1h loop would never tick
func TestStatsServer_startEmitter_reloadMovesTheInterval(t *testing.T) {
	var passes atomic.Int64
	c := config.NewC(slog.New(slog.DiscardHandler))
	setStatsConfig(c, map[string]any{"interval": "1h"})
	s := newEmitterOnlyServer(t, c, false, &passes)
	s.startEmitter()
	first := currentLoopDone(s)

	setStatsConfig(c, map[string]any{"interval": "20ms"})
	require.NoError(t, s.reload(c, false))
	assert.True(t, isClosed(first), "the old loop outlived the reload")
	waitFor(t, func() bool { return passes.Load() >= 4 })
}

// Selecting an exporter hands the emitter over and deselecting takes it back, one loop at a time
func TestStatsServer_startEmitter_reloadSwapsLoops(t *testing.T) {
	var passes atomic.Int64
	port := freeTCPPort(t)
	c := config.NewC(slog.New(slog.DiscardHandler))
	setStatsConfig(c, map[string]any{"interval": "1h"})
	s := newEmitterOnlyServer(t, c, false, &passes)

	s.startEmitter()
	require.Equal(t, int64(1), passes.Load())
	idle := currentLoopDone(s)

	setStatsConfig(c, map[string]any{
		"type":     "prometheus",
		"interval": "1h",
		"listen":   "127.0.0.1:" + port,
		"path":     "/metrics",
	})
	require.NoError(t, s.reload(c, false))
	assert.True(t, isClosed(idle), "the no-exporter loop outlived the reload")
	waitForListening(t, "127.0.0.1:"+port)
	assert.Equal(t, int64(2), passes.Load(), "the exporter runtime primes once")
	exporter := currentLoopDone(s)
	assert.NotEqual(t, idle, exporter)
	assert.False(t, isClosed(exporter))

	setStatsConfig(c, map[string]any{"interval": "1h"})
	require.NoError(t, s.reload(c, false))
	assert.True(t, isClosed(exporter), "the exporter loop outlived the reload")
	assert.Equal(t, int64(3), passes.Load(), "the no-exporter loop primes once before reload returns")
	back := currentLoopDone(s)
	assert.NotEqual(t, exporter, back)
	assert.False(t, isClosed(back))
	assert.NotNil(t, currentRuntime(s))
}
