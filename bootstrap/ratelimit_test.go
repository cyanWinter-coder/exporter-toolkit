// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/exporter-toolkit/web"
)

// writeWebConfig writes a web configuration file for tests.
func writeWebConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "web.yml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestRateLimitMetricsCollector checks that the collector publishes the web
// limiter counters, and publishes nothing when no limiter is configured.
func TestRateLimitMetricsCollector(t *testing.T) {
	t.Run("no limiter", func(t *testing.T) {
		server := &http.Server{
			Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		}
		reg := prometheus.NewRegistry()
		reg.MustRegister(newRateLimitCollector(func() *http.Server { return server }))
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		if len(families) != 0 {
			t.Errorf("got %d metric families without a limiter, want 0", len(families))
		}
	})

	t.Run("live limiter", func(t *testing.T) {
		configPath := writeWebConfig(t, "rate_limit:\n  interval: 1h\n  burst: 1\n  max_wait: 1h\n")
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Write([]byte("metrics"))
			}),
		}
		serveDone := make(chan error, 1)
		go func() {
			serveDone <- serveListenerForTest(listener, server, configPath)
		}()

		client := &http.Client{Timeout: 30 * time.Second}
		url := "http://" + listener.Addr().String() + "/metrics"

		// One admitted scrape drains the bucket.
		resp, err := client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		// One queued scrape.
		var wg sync.WaitGroup
		wg.Go(func() {
			resp, err := client.Get(url)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		})
		awaitRateLimitWaiting(t, server, 1)

		reg := prometheus.NewRegistry()
		reg.MustRegister(newRateLimitCollector(func() *http.Server { return server }))
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		byName := map[string]*dto.MetricFamily{}
		for _, f := range families {
			byName[f.GetName()] = f
		}
		if _, ok := byName["exporter_web_rate_limit_requests_total"]; !ok {
			t.Fatalf("no requests_total metric in %v", byName)
		}
		waiting := byName["exporter_web_rate_limit_waiting_requests"]
		if waiting == nil || len(waiting.Metric) != 1 || waiting.Metric[0].GetGauge().GetValue() != 1 {
			t.Fatalf("unexpected waiting metric: %v", waiting)
		}
		// The identity label must be a sanitized user or address, never a
		// credential.
		var ids []string
		if f := byName["exporter_web_rate_limit_identity_requests_total"]; f != nil {
			for _, m := range f.Metric {
				for _, l := range m.Label {
					if l.GetName() == "identity" {
						ids = append(ids, l.GetValue())
					}
				}
			}
		}
		if len(ids) == 0 {
			t.Fatal("no identity labels")
		}
		for _, id := range ids {
			if !strings.HasPrefix(id, "user:") && !strings.HasPrefix(id, "ip:") {
				t.Errorf("identity %q is not a sanitized user or address", id)
			}
			if strings.Contains(id, ":password") || strings.Contains(id, "@") {
				t.Errorf("identity %q looks like it contains credentials", id)
			}
		}

		// Shutdown releases the queued scrape and lets the test finish.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		wg.Wait()
		select {
		case err := <-serveDone:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("Serve returned %v, want %v", err, http.ErrServerClosed)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Serve did not return after shutdown")
		}
	})
}

func serveListenerForTest(l net.Listener, server *http.Server, configPath string) error {
	return web.Serve(l, server, &web.FlagConfig{WebConfigFile: &configPath}, slog.New(slog.DiscardHandler))
}

// TestHealthEndpointsBypassRateLimit checks that health and readiness probes
// keep answering while the rate limiter is saturated, so an orchestrator never
// restarts an exporter that is merely throttling scrapes.
func TestHealthEndpointsBypassRateLimit(t *testing.T) {
	configPath := writeWebConfig(t, "rate_limit:\n  interval: 1h\n  burst: 1\n  max_wait: 1h\n")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("metrics"))
	})
	// The probes respond immediately; only the metrics path is slow. This is
	// what bootstrap wires up, and the limiter must not queue the probes.
	mux.HandleFunc("/-/healthy", okHandler)
	mux.HandleFunc("/-/ready", okHandler)
	server := &http.Server{Handler: mux}
	serveDone := make(chan error, 1)
	go func() { serveDone <- serveListenerForTest(listener, server, configPath) }()

	client := &http.Client{Timeout: 30 * time.Second}
	base := "http://" + listener.Addr().String()

	// Drain the single token with a request that finishes immediately.
	resp, err := client.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("first scrape: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first scrape: got %d, want 200", resp.StatusCode)
	}

	// The next scrape queues behind the refill interval.
	var wg sync.WaitGroup
	wg.Go(func() {
		resp, err := client.Get(base + "/metrics")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
	awaitRateLimitWaiting(t, server, 1)

	for _, path := range []string{"/-/healthy", "/-/ready"} {
		resp, err := client.Get(base + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s with a saturated limiter: got %d, want 200", path, resp.StatusCode)
		}
	}

	// Shutdown releases the queued scrape; no need to wait out the interval.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	wg.Wait()
	<-serveDone
}

func awaitRateLimitWaiting(t *testing.T, server *http.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if stats, _, ok := web.ServerRateLimitStats(server); ok && stats.Waiting == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	stats, _, _ := web.ServerRateLimitStats(server)
	t.Fatalf("timed out waiting for %d queued requests, last saw %d", want, stats.Waiting)
}
