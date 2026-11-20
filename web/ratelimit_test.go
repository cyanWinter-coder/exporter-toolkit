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

package web

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// testWaitTimeout bounds how long a test waits for a queued request to be
// admitted, so a regression fails the test instead of hanging.
const testWaitTimeout = 10 * time.Second

func testLimiter(cfg RateLimiterConfig) *RateLimiter {
	return NewRateLimiter(cfg.withDefaults(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
}

// acquireInBackground starts Acquire in a goroutine and returns a channel with
// its outcome.
func acquireInBackground(ctx context.Context, l *RateLimiter, identity string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx, identity)
		done <- err
	}()
	return done
}

func awaitOutcome(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for a rate limiter decision")
		return nil
	}
}

func awaitWaiting(t *testing.T, l *RateLimiter, want int) {
	t.Helper()
	deadline := time.Now().Add(testWaitTimeout)
	for time.Now().Before(deadline) {
		if got := l.Stats().Waiting; got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d queued requests, last saw %d", want, l.Stats().Waiting)
}

func awaitWaitingAtLeast(t *testing.T, l *RateLimiter, want int) {
	t.Helper()
	deadline := time.Now().Add(testWaitTimeout)
	for time.Now().Before(deadline) {
		if got := l.Stats().Waiting; got >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for at least %d queued requests, last saw %d", want, l.Stats().Waiting)
}

// TestRateLimiterBurstThenQueue checks that requests beyond the burst are
// queued and admitted in arrival order instead of being rejected.
func TestRateLimiterBurstThenQueue(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: 50 * time.Millisecond,
		Burst:    1,
		MaxWait:  time.Second,
	})
	defer l.Close()

	// Drain the initial token.
	if _, err := l.Acquire(context.Background(), "ip:1.2.3.4"); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// Queue three requests. With one token per interval they are admitted in
	// the order they arrived.
	var wg sync.WaitGroup
	outcomes := make([]error, 3)
	for i := range outcomes {
		wg.Go(func() {
			_, outcomes[i] = l.Acquire(context.Background(), "ip:1.2.3.4")
		})
	}
	awaitWaiting(t, l, 3)
	wg.Wait()

	for i, err := range outcomes {
		if err != nil {
			t.Errorf("queued request %d: %v", i, err)
		}
	}
	stats := l.Stats()
	if stats.Allowed != 4 || stats.Delayed != 3 {
		t.Errorf("got allowed=%d delayed=%d, want 4 and 3", stats.Allowed, stats.Delayed)
	}
	if stats.Waiting != 0 {
		t.Errorf("got %d still waiting, want 0", stats.Waiting)
	}
}

// TestRateLimiterGlobalBucketBoundsAllQueues checks that separate identities
// cannot exceed the shared limit by queuing in parallel.
func TestRateLimiterGlobalBucketBoundsAllQueues(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: 40 * time.Millisecond,
		Burst:    1,
		MaxWait:  3 * time.Second,
	})
	defer l.Close()

	const identities = 4
	start := time.Now()
	var wg sync.WaitGroup
	for i := range identities {
		wg.Go(func() {
			if _, err := l.Acquire(context.Background(), fmt.Sprintf("ip:10.0.0.%d", i)); err != nil {
				t.Errorf("Acquire: %v", err)
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	// One burst token plus one per interval: 3 more admissions take at
	// least 3*40ms no matter how many identities are involved.
	if elapsed < 3*40*time.Millisecond {
		t.Errorf("admitted %d requests in %v, faster than the shared bucket allows", identities, elapsed)
	}
}

// TestRateLimiterArrivalOrderPerIdentity checks that a queue is served FIFO.
func TestRateLimiterArrivalOrderPerIdentity(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: 10 * time.Millisecond,
		Burst:    1,
		MaxWait:  time.Second,
	})
	defer l.Close()

	if _, err := l.Acquire(context.Background(), "ip:queue"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	const n = 5
	var (
		mu    sync.Mutex
		order []int
		wg    sync.WaitGroup
	)
	for i := range n {
		// Stagger starts so arrival order is deterministic.
		time.Sleep(2 * time.Millisecond)
		wg.Go(func() {
			if _, err := l.Acquire(context.Background(), "ip:queue"); err != nil {
				t.Errorf("Acquire %d: %v", i, err)
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		})
	}
	wg.Wait()

	for i := 1; i < len(order); i++ {
		if order[i] < order[i-1] {
			t.Fatalf("requests served out of arrival order: %v", order)
		}
	}
}

// TestRateLimiterTimeout checks that a request that cannot be admitted within
// max_wait is rejected with ErrRateLimitTimeout after waiting the full
// deadline, and leaves the queue.
func TestRateLimiterTimeout(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: 5 * time.Second,
		Burst:    1,
		MaxWait:  60 * time.Millisecond,
	})
	defer l.Close()

	if _, err := l.Acquire(context.Background(), "ip:slow"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	start := time.Now()
	_, err := l.Acquire(context.Background(), "ip:slow")
	elapsed := time.Since(start)
	if !errors.Is(err, ErrRateLimitTimeout) {
		t.Fatalf("got %v, want %v", err, ErrRateLimitTimeout)
	}
	if elapsed < 50*time.Millisecond || elapsed > time.Second {
		t.Errorf("rejected after %v, want the configured max_wait of 60ms", elapsed)
	}
	if stats := l.Stats(); stats.Waiting != 0 {
		t.Errorf("got %d waiting after timeout, want 0", stats.Waiting)
	}
	if stats := l.Stats(); stats.Rejected != 1 {
		t.Errorf("got %d rejected, want 1", stats.Rejected)
	}
}

// TestRateLimiterZeroBurstRejectsWithoutWaiting checks that a bucket that can
// never admit a request fails fast instead of making every client wait out
// max_wait.
func TestRateLimiterZeroBurstRejectsWithoutWaiting(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: time.Second,
		Burst:    0,
		MaxWait:  time.Hour,
	})
	defer l.Close()

	start := time.Now()
	_, err := l.Acquire(context.Background(), "ip:zero")
	if !errors.Is(err, ErrRateLimitExceeded) {
		t.Fatalf("got %v, want %v", err, ErrRateLimitExceeded)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("rejected after %v instead of immediately", elapsed)
	}
}

// TestRateLimiterWaitUntilDeadline checks that a request which can be served
// before its deadline does wait for that token instead of being rejected.
func TestRateLimiterWaitUntilDeadline(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: 40 * time.Millisecond,
		Burst:    1,
		MaxWait:  time.Second,
	})
	defer l.Close()

	if _, err := l.Acquire(context.Background(), "ip:punctual"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	start := time.Now()
	if _, err := l.Acquire(context.Background(), "ip:punctual"); err != nil {
		t.Fatalf("queued request: %v", err)
	}
	if waited := time.Since(start); waited < 30*time.Millisecond {
		t.Errorf("admitted after %v, before the next token was due", waited)
	}
}

// TestRateLimiterCancelLeavesQueueImmediately checks that a canceled scrape
// leaves its queue at once, is not counted as a rejection, and does not spend
// the token the scheduler had reserved for it.
func TestRateLimiterCancelLeavesQueueImmediately(t *testing.T) {
	const interval = 80 * time.Millisecond
	l := testLimiter(RateLimiterConfig{
		Interval: interval,
		Burst:    1,
		MaxWait:  time.Hour,
	})
	defer l.Close()

	if _, err := l.Acquire(context.Background(), "ip:cancel"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	// Queue a request and cancel it while the scheduler is waiting for the
	// next token.
	ctx, cancel := context.WithCancel(context.Background())
	done := acquireInBackground(ctx, l, "ip:cancel")
	awaitWaiting(t, l, 1)
	cancel()
	if err := awaitOutcome(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want %v", err, context.Canceled)
	}
	if stats := l.Stats(); stats.Waiting != 0 || stats.Canceled != 1 || stats.Rejected != 0 {
		t.Errorf("got waiting=%d canceled=%d rejected=%d, want 0, 1 and 0",
			stats.Waiting, stats.Canceled, stats.Rejected)
	}

	// The canceled request must not consume the token reserved for it. A
	// fresh request is admitted after one interval, the delay of the token
	// that was returned, rather than after two.
	time.Sleep(10 * time.Millisecond)
	start := time.Now()
	if _, err := l.Acquire(context.Background(), "ip:other"); err != nil {
		t.Fatalf("fresh request: %v", err)
	}
	waited := time.Since(start)
	if waited > 2*interval {
		t.Errorf("fresh request waited %v; the canceled request kept its token", waited)
	}
}

// TestRateLimiterCancelMiddleWaiter checks that canceling a waiter that is not
// at the head removes just that waiter, leaving the rest of the queue intact.
func TestRateLimiterCancelMiddleWaiter(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: time.Hour,
		Burst:    1,
		MaxWait:  time.Hour,
	})
	defer l.Close()

	if _, err := l.Acquire(context.Background(), "ip:q"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	head := acquireInBackground(context.Background(), l, "ip:q")
	awaitWaiting(t, l, 1)
	ctx, cancel := context.WithCancel(context.Background())
	middle := acquireInBackground(ctx, l, "ip:q")
	awaitWaiting(t, l, 2)
	tail := acquireInBackground(context.Background(), l, "ip:q")
	awaitWaiting(t, l, 3)

	cancel()
	if err := awaitOutcome(t, middle); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter: got %v, want %v", err, context.Canceled)
	}
	if got := l.Stats().Waiting; got != 2 {
		t.Fatalf("got %d waiting after canceling the middle waiter, want 2", got)
	}

	l.Close()
	for name, ch := range map[string]<-chan error{"head": head, "tail": tail} {
		if err := awaitOutcome(t, ch); !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("%s waiter: got %v, want %v", name, err, ErrRateLimiterClosed)
		}
	}
}

// TestRateLimiterCloseReleasesWaiters checks that closing the limiter wakes
// every queued request and rejects new ones.
func TestRateLimiterCloseReleasesWaiters(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: time.Hour,
		Burst:    1,
		MaxWait:  time.Hour,
	})

	if _, err := l.Acquire(context.Background(), "ip:close"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	const waiters = 5
	done := make([]<-chan error, waiters)
	for i := range done {
		done[i] = acquireInBackground(context.Background(), l, fmt.Sprintf("ip:%d", i%2))
	}
	awaitWaiting(t, l, waiters)

	l.Close()

	for i, ch := range done {
		if err := awaitOutcome(t, ch); !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("waiter %d: got %v, want %v", i, err, ErrRateLimiterClosed)
		}
	}
	if _, err := l.Acquire(context.Background(), "ip:after"); !errors.Is(err, ErrRateLimiterClosed) {
		t.Errorf("Acquire after Close: got %v, want %v", err, ErrRateLimiterClosed)
	}
	l.Close() // idempotent.
}

// TestRateLimiterQueueFull checks the per-identity queue bound.
func TestRateLimiterQueueFull(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval:  time.Hour,
		Burst:     1,
		MaxWait:   time.Hour,
		MaxQueued: 1,
	})
	defer l.Close()

	if _, err := l.Acquire(context.Background(), "ip:full"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	done := acquireInBackground(context.Background(), l, "ip:full")
	awaitWaiting(t, l, 1)

	if _, err := l.Acquire(context.Background(), "ip:full"); !errors.Is(err, ErrRateLimitQueueFull) {
		t.Fatalf("got %v, want %v", err, ErrRateLimitQueueFull)
	}
	// Another identity still has room: it is not rejected by the bound that
	// applies to ip:full, it waits like any other request.
	roomy := acquireInBackground(context.Background(), l, "ip:roomy")
	awaitWaiting(t, l, 2)
	if s, ok := l.IdentityStats()["ip:roomy"]; !ok || s.Waiting != 1 {
		t.Errorf("ip:roomy is not queued: %+v", l.IdentityStats())
	}

	l.Close()
	for i, ch := range []<-chan error{roomy, done} {
		if err := awaitOutcome(t, ch); !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("waiter %d: got %v, want %v", i, err, ErrRateLimiterClosed)
		}
	}
}

// TestRateLimiterNoQueueImmediateRejection checks that a negative max_wait
// restores the immediate-rejection behaviour.
func TestRateLimiterNoQueueImmediateRejection(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: time.Hour,
		Burst:    2,
		MaxWait:  -1,
	})
	defer l.Close()

	for range 2 {
		if _, err := l.Acquire(context.Background(), "ip:now"); err != nil {
			t.Fatalf("burst token: %v", err)
		}
	}
	if _, err := l.Acquire(context.Background(), "ip:now"); !errors.Is(err, ErrRateLimitExceeded) {
		t.Fatalf("got %v, want %v", err, ErrRateLimitExceeded)
	}
}

// TestRateLimiterNoStarvation checks that a busy identity does not starve a
// quiet one: the scheduler rotates between queues.
func TestRateLimiterNoStarvation(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: 5 * time.Millisecond,
		Burst:    1,
		MaxWait:  5 * time.Second,
	})
	defer l.Close()

	if _, err := l.Acquire(context.Background(), "ip:busy"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	// Queue a backlog behind the busy identity. With a slow interval the
	// backlog stays at its maximum long enough to observe.
	l2 := testLimiter(RateLimiterConfig{
		Interval: 20 * time.Millisecond,
		Burst:    1,
		MaxWait:  5 * time.Second,
	})
	defer l2.Close()
	if _, err := l2.Acquire(context.Background(), "ip:busy"); err != nil {
		t.Fatalf("drain bucket: %v", err)
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, err := l2.Acquire(context.Background(), "ip:busy"); err != nil {
				t.Errorf("busy: %v", err)
			}
		})
	}
	awaitWaitingAtLeast(t, l2, 1)

	start := time.Now()
	if _, err := l2.Acquire(context.Background(), "ip:quiet"); err != nil {
		t.Fatalf("quiet identity: %v", err)
	}
	// The quiet request is served after at most one turn of another
	// identity, not after the ten-request backlog.
	if waited := time.Since(start); waited > time.Second {
		t.Errorf("quiet identity waited %v behind a busy queue", waited)
	}
	wg.Wait()
}

// TestRateLimiterStatsAndIdentities checks the observability counters.
func TestRateLimiterStatsAndIdentities(t *testing.T) {
	logs := &syncBuffer{}
	l := NewRateLimiter(RateLimiterConfig{
		Interval: 20 * time.Millisecond,
		Burst:    1,
		MaxWait:  time.Second,
	}, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer l.Close()

	if _, err := l.Acquire(context.Background(), "user:alice"); err != nil {
		t.Fatal(err)
	}
	done := acquireInBackground(context.Background(), l, "user:alice")
	if err := awaitOutcome(t, done); err != nil {
		t.Fatal(err)
	}

	stats := l.Stats()
	if stats.Allowed != 2 || stats.Delayed != 1 {
		t.Errorf("got allowed=%d delayed=%d, want 2 and 1", stats.Allowed, stats.Delayed)
	}
	if stats.TotalWait <= 0 {
		t.Errorf("got total wait %v, want >0", stats.TotalWait)
	}
	perIdentity := l.IdentityStats()
	alice, ok := perIdentity["user:alice"]
	if !ok {
		t.Fatalf("no stats for user:alice in %v", perIdentity)
	}
	if alice.Allowed != 2 || alice.Delayed != 1 {
		t.Errorf("identity stats: got allowed=%d delayed=%d, want 2 and 1", alice.Allowed, alice.Delayed)
	}

	if !strings.Contains(logs.String(), "Scrape request rate limiter decision") {
		t.Errorf("no decision logged: %q", logs.String())
	}
	// The summary must not include credential material.
	for _, secret := range []string{"password", "Authorization", "secret"} {
		if strings.Contains(strings.ToLower(logs.String()), strings.ToLower(secret)) {
			t.Errorf("log contains %q: %q", secret, logs.String())
		}
	}
}

// TestRateLimiterExemptPaths checks the health exemptions.
func TestRateLimiterExemptPaths(t *testing.T) {
	l := NewRateLimiter(RateLimiterConfig{
		Interval:    time.Hour,
		Burst:       1,
		ExemptPaths: []string{"/custom-health", "/debug/*"},
	}, nil)
	defer l.Close()

	for _, path := range []string{"/-/healthy", "/-/ready", "/healthz", "/custom-health", "/debug/pprof/"} {
		if !l.Exempt(path) {
			t.Errorf("%s should be exempt", path)
		}
	}
	for _, path := range []string{"/metrics", "/debug", "/debugger"} {
		if l.Exempt(path) {
			t.Errorf("%s should not be exempt", path)
		}
	}

	var nilLimiter *RateLimiter
	if !nilLimiter.Exempt("/anything") {
		t.Error("nil limiter should exempt everything")
	}
	if nilLimiter.Stats() != (RateLimitStats{}) {
		t.Error("nil limiter should return zero stats")
	}
	if _, err := nilLimiter.Acquire(context.Background(), "user:x"); err != nil {
		t.Errorf("nil limiter rejected a request: %v", err)
	}
	nilLimiter.Close()
}

// TestRateLimiterRetryAfter checks that Retry-After is sane and bounded.
func TestRateLimiterRetryAfter(t *testing.T) {
	l := testLimiter(RateLimiterConfig{Interval: 250 * time.Millisecond, Burst: 2})
	defer l.Close()

	if got := retryAfterSeconds(l.RetryAfter()); got != "1" {
		t.Errorf("Retry-After = %s, want 1", got)
	}

	// Exhaust the bucket and ensure the advertised delay covers the wait.
	for range 2 {
		if _, err := l.Acquire(context.Background(), "ip:r"); err != nil {
			t.Fatal(err)
		}
	}
	if got := retryAfterSeconds(l.RetryAfter()); got == "0" {
		t.Error("Retry-After = 0, want at least 1")
	}

	// A zero-burst limiter can never admit anything; Retry-After must still
	// be usable.
	zero := testLimiter(RateLimiterConfig{Interval: time.Second, Burst: 0})
	defer zero.Close()
	if got := retryAfterSeconds(zero.RetryAfter()); got != "1" {
		t.Errorf("Retry-After = %s, want 1", got)
	}
}

// TestRateLimiterConfigValidation checks that impossible configurations are
// rejected when the config is loaded.
func TestRateLimiterConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     RateLimiterConfig
		wantErr bool
	}{
		{name: "zero interval disables", cfg: RateLimiterConfig{}},
		{name: "negative interval", cfg: RateLimiterConfig{Interval: -time.Second}, wantErr: true},
		{name: "negative burst", cfg: RateLimiterConfig{Burst: -1}, wantErr: true},
		{name: "negative max queued", cfg: RateLimiterConfig{MaxQueued: -1}, wantErr: true},
		{name: "max queued without queueing", cfg: RateLimiterConfig{MaxQueued: 5, MaxWait: -1}, wantErr: true},
		{name: "defaults", cfg: RateLimiterConfig{Interval: time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestHandlerQueueingEndToEnd drives the web handler through the rate limiter
// and checks the HTTP responses: queued scrapes are served, expired ones get
// 429 with Retry-After, and health paths stay exempt.
func TestHandlerQueueingEndToEnd(t *testing.T) {
	dir := t.TempDir()
	configPath := dir + "/web.yml"
	writeFile(t, configPath, "rate_limit:\n  interval: 30ms\n  burst: 1\n  max_wait: 5s\n")

	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	h := &webHandler{
		tlsConfigPath: configPath,
		logger:        testlogger,
		cache:         newCache(),
		rateLimiter:   NewRateLimiter(RateLimiterConfig{Interval: 30 * time.Millisecond, Burst: 1, MaxWait: 5 * time.Second}, testlogger),
		handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			entered <- struct{}{}
			<-release
			w.Write([]byte("metrics"))
		}),
	}
	defer h.rateLimiter.Close()

	const scrapes = 3
	var wg sync.WaitGroup
	codes := make([]int, scrapes)
	for i := range scrapes {
		wg.Go(func() {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			codes[i] = rec.Code
		})
	}
	// One request is admitted immediately, the rest queue.
	awaitSignal(t, entered)
	close(release)
	wg.Wait()
	for i, code := range codes {
		if code != http.StatusOK {
			t.Errorf("scrape %d: got %d, want 200", i, code)
		}
	}

	// Health paths bypass the limiter even when the bucket is empty.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/-/healthy", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/-/healthy: got %d, want 200", rec.Code)
	}
}

// TestHandlerRateLimitRejectionHasRetryAfter checks the 429 response shape.
func TestHandlerRateLimitRejectionHasRetryAfter(t *testing.T) {
	dir := t.TempDir()
	configPath := dir + "/web.yml"
	writeFile(t, configPath, "rate_limit:\n  interval: 1s\n  burst: 0\n")

	h := &webHandler{
		tlsConfigPath: configPath,
		logger:        testlogger,
		cache:         newCache(),
		rateLimiter:   NewRateLimiter(RateLimiterConfig{Interval: time.Second, Burst: 0, MaxWait: time.Second}, testlogger),
		handler:       http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }),
	}
	defer h.rateLimiter.Close()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("got %d, want %d: %s", rec.Code, http.StatusTooManyRequests, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q, want a positive number of seconds", got)
	}
}

// TestHandlerUnauthenticatedQueueByAddress checks that unauthorized requests
// share one queue per source address and do not name their own identity.
func TestHandlerUnauthenticatedQueueByAddress(t *testing.T) {
	dir := t.TempDir()
	configPath := dir + "/web.yml"
	writeFile(t, configPath, "rate_limit:\n  interval: 1h\n  burst: 10\nbasic_auth_users:\n  alice: $2y$10$qRTBuFoULoYNA7AQ/F3ck.trZBPyjV64.oA4ZsSBCIWvXuvQlQTuu\n")

	h := &webHandler{
		tlsConfigPath: configPath,
		logger:        testlogger,
		cache:         newCache(),
		rateLimiter:   NewRateLimiter(RateLimiterConfig{Interval: time.Hour, Burst: 10, MaxWait: time.Second}, testlogger),
		handler:       http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }),
	}
	defer h.rateLimiter.Close()

	// An unknown user falls back to the address queue; a request naming a
	// configured user shares that user's queue even when the password is
	// wrong, because the limiter runs before the bcrypt check.
	for _, user := range []string{"mallory", "alice"} {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		req.RemoteAddr = "192.0.2.9:1234"
		req.SetBasicAuth(user, "wrong")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: got %d, want 401", user, rec.Code)
		}
	}

	perIdentity := h.rateLimiter.IdentityStats()
	if _, ok := perIdentity["ip:192.0.2.9"]; !ok {
		t.Errorf("no address queue for the unknown user: %v", perIdentity)
	}
	if _, ok := perIdentity["user:alice"]; !ok {
		t.Errorf("no user queue for a configured user: %v", perIdentity)
	}
	if _, ok := perIdentity["user:mallory"]; ok {
		t.Errorf("unverified user name became a queue identity: %v", perIdentity)
	}
}

// TestServerShutdownReleasesQueuedScrapes checks that shutting down the HTTP
// server wakes every queued request with 503 instead of leaving scrapes
// waiting for max_wait.
func TestServerShutdownReleasesQueuedScrapes(t *testing.T) {
	dir := t.TempDir()
	configPath := dir + "/web.yml"
	writeFile(t, configPath, "rate_limit:\n  interval: 1h\n  burst: 1\n  max_wait: 1h\n")

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
		serveDone <- Serve(listener, server, &FlagConfig{WebConfigFile: &configPath}, testlogger)
	}()

	url := "http://" + listener.Addr().String() + "/metrics"
	client := &http.Client{Timeout: 30 * time.Second}
	get := func() (*http.Response, error) {
		resp, err := client.Get(url)
		if err != nil {
			return nil, err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp, nil
	}

	// Drain the single token.
	resp, err := get()
	if err != nil {
		t.Fatalf("first scrape: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first scrape: got %d, want 200", resp.StatusCode)
	}

	const queued = 3
	type result struct {
		status     int
		retryAfter string
	}
	results := make(chan result, queued)
	for range queued {
		go func() {
			resp, err := get()
			if err != nil {
				results <- result{status: -1}
				return
			}
			results <- result{status: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After")}
		}()
	}

	awaitStatsWaiting(t, server, queued)

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(context.Background()) }()

	for i := range queued {
		select {
		case got := <-results:
			if got.status != http.StatusServiceUnavailable {
				t.Errorf("queued scrape %d after shutdown: got %d, want 503", i, got.status)
			}
			if got.retryAfter == "" {
				t.Errorf("queued scrape %d after shutdown has no Retry-After", i)
			}
		case <-time.After(testWaitTimeout):
			t.Fatalf("queued scrape %d was not released by shutdown", i)
		}
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("Shutdown did not complete")
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Serve returned %v, want %v", err, http.ErrServerClosed)
	}
	if stats, _, ok := ServerRateLimitStats(server); !ok || stats.Waiting != 0 {
		t.Errorf("got %d requests still queued after shutdown, want 0", stats.Waiting)
	}
}

func awaitStatsWaiting(t *testing.T, server *http.Server, want int) RateLimitStats {
	t.Helper()
	deadline := time.Now().Add(testWaitTimeout)
	var stats RateLimitStats
	for time.Now().Before(deadline) {
		var ok bool
		stats, _, ok = ServerRateLimitStats(server)
		if ok && stats.Waiting == want {
			return stats
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d queued requests, last saw %d", want, stats.Waiting)
	return stats
}

// TestRateLimiterConfigYAML checks that the new options load, stay optional,
// and are validated.
func TestRateLimiterConfigYAML(t *testing.T) {
	for _, tc := range []struct {
		name    string
		yaml    string
		want    RateLimiterConfig
		wantErr string
	}{
		{
			name: "legacy configuration",
			yaml: "rate_limit:\n  interval: 1s\n  burst: 20\n",
			want: RateLimiterConfig{Interval: time.Second, Burst: 20},
		},
		{
			name: "queueing options",
			yaml: "rate_limit:\n  interval: 250ms\n  burst: 5\n  max_wait: 2s\n  max_queued: 10\n  exempt_paths:\n    - /custom\n",
			want: RateLimiterConfig{
				Interval:    250 * time.Millisecond,
				Burst:       5,
				MaxWait:     2 * time.Second,
				MaxQueued:   10,
				ExemptPaths: []string{"/custom"},
			},
		},
		{
			name: "queueing can be disabled",
			yaml: "rate_limit:\n  interval: 1s\n  burst: 1\n  max_wait: -1s\n",
			want: RateLimiterConfig{Interval: time.Second, Burst: 1, MaxWait: -time.Second},
		},
		{
			name:    "queue bound without queueing",
			yaml:    "rate_limit:\n  interval: 1s\n  burst: 1\n  max_wait: -1s\n  max_queued: 2\n",
			wantErr: "max_queued requires queueing",
		},
		{
			name:    "negative interval",
			yaml:    "rate_limit:\n  interval: -1s\n",
			wantErr: "interval must not be negative",
		},
		{
			name:    "unknown key",
			yaml:    "rate_limit:\n  interval: 1s\n  queue: yes\n",
			wantErr: "field queue not found",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := dir + "/web.yml"
			writeFile(t, path, tc.yaml)
			c, err := getConfig(path)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("got no error, want one matching %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got error %v, want one matching %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("getConfig: %v", err)
			}
			got := c.RateLimiterConfig
			if got.Interval != tc.want.Interval || got.Burst != tc.want.Burst ||
				got.MaxWait != tc.want.MaxWait || got.MaxQueued != tc.want.MaxQueued ||
				strings.Join(got.ExemptPaths, ",") != strings.Join(tc.want.ExemptPaths, ",") {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestRateLimiterCanceledContext checks that a request whose context is
// already canceled is refused before it queues, and counts as canceled rather
// than rejected.
func TestRateLimiterCanceledContext(t *testing.T) {
	l := testLimiter(RateLimiterConfig{Interval: time.Hour, Burst: 1, MaxWait: time.Hour})
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Acquire(ctx, "ip:gone"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want %v", err, context.Canceled)
	}
	stats := l.Stats()
	if stats.Canceled != 1 || stats.Waiting != 0 || stats.Rejected != 0 || stats.Allowed != 0 {
		t.Errorf("got canceled=%d waiting=%d rejected=%d allowed=%d, want 1, 0, 0 and 0",
			stats.Canceled, stats.Waiting, stats.Rejected, stats.Allowed)
	}
	// The token is untouched, so a live request is admitted at once.
	if _, err := l.Acquire(context.Background(), "ip:live"); err != nil {
		t.Fatalf("live request: %v", err)
	}
}

// TestRateLimiterBoundsTrackedIdentities checks that a flood of distinct
// source addresses cannot grow the per-identity accounting without bound,
// while aggregate counters stay correct.
func TestRateLimiterBoundsTrackedIdentities(t *testing.T) {
	l := testLimiter(RateLimiterConfig{
		Interval: time.Hour,
		Burst:    10000,
		MaxWait:  -1,
	})
	defer l.Close()

	const requests = maxTrackedIdentities + 200
	for i := range requests {
		if _, err := l.Acquire(context.Background(), fmt.Sprintf("ip:198.51.%d.%d", i/256, i%256)); err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
	}

	identities := l.IdentityStats()
	if len(identities) > maxTrackedIdentities {
		t.Errorf("tracked %d identities, want at most %d", len(identities), maxTrackedIdentities)
	}
	if stats := l.Stats(); stats.Allowed != requests {
		t.Errorf("got %d allowed, want %d", stats.Allowed, requests)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for a request to reach the handler")
	}
}
