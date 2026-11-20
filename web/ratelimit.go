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
	"container/list"
	"context"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

var (
	// ErrRateLimitExceeded is returned when a request cannot be admitted
	// immediately and queueing is disabled, or when the bucket is too small
	// to ever admit it. Handlers answer it with 429 Too Many Requests.
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
	// ErrRateLimitTimeout is returned when a request waited for its maximum
	// wait time without a token becoming available.
	ErrRateLimitTimeout = errors.New("rate limit wait timeout exceeded")
	// ErrRateLimitQueueFull is returned when the per-identity queue is full.
	ErrRateLimitQueueFull = errors.New("rate limit queue is full")
	// ErrRateLimiterClosed is returned when the limiter is shutting down.
	ErrRateLimiterClosed = errors.New("rate limiter is closed")
)

// maxTrackedIdentities bounds the number of per-identity counter entries kept.
// Unauthenticated traffic creates one identity per source address, so an
// unbounded map would let a source-address flood grow memory without limit.
// Aggregate counters are unaffected by the bound.
const maxTrackedIdentities = 1024

// defaultRateLimitExemptPaths are served without going through the rate
// limiter. They cover the liveness and readiness probes provided by the
// bootstrap package plus the health paths commonly registered by exporters.
var defaultRateLimitExemptPaths = []string{
	"/-/healthy",
	"/-/ready",
	"/-/health",
	"/-/live",
	"/healthz",
	"/livez",
	"/readyz",
	"/health",
	"/ready",
}

// RateLimitStats is a point-in-time snapshot of the rate limiter counters.
type RateLimitStats struct {
	// Allowed counts admitted requests, both immediate and delayed.
	Allowed uint64
	// Delayed counts requests admitted after waiting in a queue.
	Delayed uint64
	// Rejected counts requests answered with 429 (or 503 on shutdown).
	Rejected uint64
	// Canceled counts waiting requests whose client went away.
	Canceled uint64
	// Waiting is the number of requests currently queued.
	Waiting int
	// TotalWait is the cumulative time requests spent queued before being
	// admitted or rejected.
	TotalWait time.Duration
}

// IdentityStats is the RateLimitStats equivalent for a single queue identity.
type IdentityStats struct {
	Allowed   uint64
	Delayed   uint64
	Rejected  uint64
	Waiting   int
	TotalWait time.Duration
}

// rateWaiter is one request waiting for a token. All fields except done are
// guarded by RateLimiter.mu. done is closed once the waiter is resolved, which
// publishes the other fields to Acquire.
type rateWaiter struct {
	key      string
	enqueued time.Time
	deadline time.Time
	elem     *list.Element
	done     chan struct{}
	resolved bool
	granted  bool
	err      error
	wait     time.Duration
}

type identityCounters struct {
	allowed  uint64
	delayed  uint64
	rejected uint64
	wait     time.Duration
}

// RateLimiter bounds how many requests are admitted per interval while
// queueing requests that arrive without a token. Requests are queued per
// identity: the authenticated user name when basic authentication is
// configured, and the client IP address otherwise. A single token bucket
// bounds the combined capacity of all queues, so adding identities or opening
// new connections cannot raise the overall request rate.
//
// Each queue is served in arrival order. The scheduler rotates between
// identities so that one busy client cannot starve the others. Requests that
// cannot be admitted within the configured maximum wait are rejected.
//
// RateLimiter is safe for concurrent use.
type RateLimiter struct {
	cfg            RateLimiterConfig
	logger         *slog.Logger
	limiter        *rate.Limiter
	exempt         map[string]struct{}
	exemptPrefixes map[string]bool

	startOnce sync.Once
	wake      chan struct{}
	closeCh   chan struct{}

	// logMu serializes log writes. Decisions are taken from several
	// goroutines, and this keeps their records from interleaving.
	logMu sync.Mutex

	mu       sync.Mutex
	queues   map[string]*list.List
	order    []string
	next     int
	waiting  int
	closed   bool
	allowed  uint64
	delayed  uint64
	rejected uint64
	canceled uint64
	wait     time.Duration
	byID     map[string]*identityCounters
}

// NewRateLimiter returns a limiter for cfg. It returns nil when the limiter is
// disabled, which is the case when cfg.Interval is zero. A nil *RateLimiter is
// valid and admits every request.
func NewRateLimiter(cfg RateLimiterConfig, logger *slog.Logger) *RateLimiter {
	if cfg.Interval <= 0 {
		return nil
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	exempt := make(map[string]struct{}, len(defaultRateLimitExemptPaths)+len(cfg.ExemptPaths))
	prefixes := make(map[string]bool, len(cfg.ExemptPaths))
	for _, path := range defaultRateLimitExemptPaths {
		exempt[path] = struct{}{}
	}
	for _, path := range cfg.ExemptPaths {
		if path == "" {
			continue
		}
		if prefix, ok := strings.CutSuffix(path, "*"); ok {
			prefixes[prefix] = true
			continue
		}
		exempt[path] = struct{}{}
	}
	return &RateLimiter{
		cfg:            cfg,
		logger:         logger,
		limiter:        rate.NewLimiter(rate.Every(cfg.Interval), cfg.Burst),
		exempt:         exempt,
		exemptPrefixes: prefixes,
		wake:           make(chan struct{}, 1),
		closeCh:        make(chan struct{}),
		queues:         make(map[string]*list.List),
		byID:           make(map[string]*identityCounters),
	}
}

// Exempt reports whether path bypasses the rate limiter. The probes registered
// by the bootstrap package are always exempt, along with exempt_paths from the
// configuration. A trailing "*" makes a configured prefix exempt a subtree.
func (l *RateLimiter) Exempt(path string) bool {
	if l == nil {
		return true
	}
	if _, ok := l.exempt[path]; ok {
		return true
	}
	for prefix, isPrefix := range l.exemptPrefixes {
		if isPrefix && strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// RetryAfter returns how long a client should wait before retrying. The value
// is rounded up to whole seconds when it is sent in the Retry-After header.
//
// It does not touch the token bucket: reserving and canceling a token would
// race with the scheduler and briefly inflate the delay for queued requests.
// Instead, the wait is derived from the queue depth, since admitting the
// waiting requests takes a token per interval.
func (l *RateLimiter) RetryAfter() time.Duration {
	if l == nil {
		return 0
	}
	if l.cfg.Interval <= 0 {
		return time.Second
	}
	l.mu.Lock()
	// One interval for the next token, plus one for each request ahead in the
	// worst-case queue.
	ahead := l.waiting
	l.mu.Unlock()
	delay := time.Duration(ahead+1) * l.cfg.Interval
	// Bound the value so a pathological configuration cannot send an
	// absurd Retry-After.
	if maxRetry := 24 * time.Hour; delay > maxRetry {
		delay = maxRetry
	}
	return delay
}

// Stats returns a snapshot of the counters. It does not reset them. It is safe
// to call on a nil limiter, which returns zero values.
func (l *RateLimiter) Stats() RateLimitStats {
	if l == nil {
		return RateLimitStats{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return RateLimitStats{
		Allowed:   l.allowed,
		Delayed:   l.delayed,
		Rejected:  l.rejected,
		Canceled:  l.canceled,
		Waiting:   l.waiting,
		TotalWait: l.wait,
	}
}

// IdentityStats returns per-identity counter snapshots. The map keys are the
// same sanitized identities used for logging: "user:<name>" for authenticated
// requests and "ip:<address>" otherwise. No credentials are included.
//
// The number of tracked identities is bounded; once the bound is reached,
// counters for identities with nothing queued are recycled, so very high
// cardinality traffic is accounted for in the aggregate Stats rather than
// individually.
func (l *RateLimiter) IdentityStats() map[string]IdentityStats {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	stats := make(map[string]IdentityStats, len(l.byID))
	for id, c := range l.byID {
		s := IdentityStats{
			Allowed:   c.allowed,
			Delayed:   c.delayed,
			Rejected:  c.rejected,
			TotalWait: c.wait,
		}
		if q := l.queues[id]; q != nil {
			s.Waiting = q.Len()
		}
		stats[id] = s
	}
	// Include identities that only have requests in flight, which have not
	// produced a counter update yet.
	for id, q := range l.queues {
		if _, ok := stats[id]; ok {
			continue
		}
		stats[id] = IdentityStats{Waiting: q.Len()}
	}
	return stats
}

// Acquire waits for permission to serve one request for identity. It returns
// the time spent waiting when the request was admitted after queueing.
//
// Acquire returns ErrRateLimitExceeded when queueing is disabled,
// ErrRateLimitQueueFull when identity already has max_queued requests waiting,
// ErrRateLimitTimeout when no token became available within max_wait,
// ErrRateLimiterClosed on shutdown, or ctx.Err() when the request was
// canceled. A canceled request leaves its queue immediately and does not
// consume a token.
func (l *RateLimiter) Acquire(ctx context.Context, identity string) (time.Duration, error) {
	if l == nil {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		l.countCanceled(identity, 0)
		return 0, err
	}
	l.start()

	now := time.Now()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, ErrRateLimiterClosed
	}
	// Only bypass the queue when nobody is waiting. Otherwise a fresh request
	// could overtake queued ones and break arrival order.
	if l.waiting == 0 && l.limiter.Allow() {
		l.countAllowedLocked(identity, 0, false)
		dec := l.newDecisionLocked(identity, "allowed", 0)
		l.mu.Unlock()
		dec.emit(l)
		return 0, nil
	}
	if l.cfg.MaxWait <= 0 {
		l.rejected++
		l.countRejectedLocked(identity, 0)
		dec := l.newDecisionLocked(identity, "rejected", 0, "reason", ErrRateLimitExceeded.Error())
		l.mu.Unlock()
		dec.emit(l)
		return 0, ErrRateLimitExceeded
	}
	if l.cfg.MaxQueued > 0 {
		if q := l.queues[identity]; q != nil && q.Len() >= l.cfg.MaxQueued {
			l.rejected++
			l.countRejectedLocked(identity, 0)
			dec := l.newDecisionLocked(identity, "rejected", 0, "reason", ErrRateLimitQueueFull.Error())
			l.mu.Unlock()
			dec.emit(l)
			return 0, ErrRateLimitQueueFull
		}
	}

	w := &rateWaiter{
		key:      identity,
		enqueued: now,
		deadline: now.Add(l.cfg.MaxWait),
		done:     make(chan struct{}),
	}
	q := l.queues[identity]
	if q == nil {
		q = list.New()
		l.queues[identity] = q
		l.order = append(l.order, identity)
	}
	w.elem = q.PushBack(w)
	l.waiting++
	l.signalLocked()
	l.mu.Unlock()

	timer := time.NewTimer(l.cfg.MaxWait)
	defer timer.Stop()

	select {
	case <-w.done:
		return l.waiterResult(w)
	case <-ctx.Done():
		if l.resolveCanceled(w, ctx.Err()) {
			return 0, ctx.Err()
		}
		// The scheduler admitted the request before observing the
		// cancellation. The token was spent, so serve it.
		if wait, err := l.waiterResult(w); err == nil {
			return wait, nil
		}
		return 0, ctx.Err()
	case <-timer.C:
		l.resolve(w, ErrRateLimitTimeout)
		return l.waiterResult(w)
	case <-l.closeCh:
		l.resolve(w, ErrRateLimiterClosed)
		return l.waiterResult(w)
	}
}

// Close wakes every waiting request and causes new requests to fail with
// ErrRateLimiterClosed. It is idempotent.
func (l *RateLimiter) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	close(l.closeCh)

	var decisions []decision
	now := time.Now()
	for _, q := range l.queues {
		for e := q.Front(); e != nil; e = e.Next() {
			w := e.Value.(*rateWaiter)
			wait := now.Sub(w.enqueued)
			l.rejected++
			l.countRejectedLocked(w.key, wait)
			decisions = append(decisions, l.newDecisionLocked(w.key, "rejected", wait, "reason", ErrRateLimiterClosed.Error()))
			l.resolveLocked(w, ErrRateLimiterClosed)
		}
	}
	released := len(decisions)
	l.queues = make(map[string]*list.List)
	l.order = nil
	l.next = 0
	l.waiting = 0
	wait := l.wait
	allowed, delayed, rejected, canceled := l.allowed, l.delayed, l.rejected, l.canceled
	l.mu.Unlock()

	for _, dec := range decisions {
		dec.emit(l)
	}
	l.logMu.Lock()
	l.logger.Info("Rate limiter shut down, released waiting requests",
		"waiting", released,
		"allowed", allowed,
		"delayed", delayed,
		"rejected", rejected,
		"canceled", canceled,
		"total_wait", wait,
	)
	l.logMu.Unlock()
}

func (l *RateLimiter) start() {
	l.startOnce.Do(func() {
		go l.run()
	})
}

// run is the single scheduler. It reserves at most one future token at a time
// and grants it to the head of the queue selected in round-robin order.
func (l *RateLimiter) run() {
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return
		}
		key, w := l.peekHeadLocked()
		if w == nil {
			l.mu.Unlock()
			select {
			case <-l.wake:
			case <-l.closeCh:
				return
			}
			continue
		}

		now := time.Now()
		if !now.Before(w.deadline) {
			dec := l.rejectHeadLocked(key, w, ErrRateLimitTimeout, now.Sub(w.enqueued))
			l.mu.Unlock()
			dec.emit(l)
			continue
		}
		r := l.limiter.ReserveN(now, 1)
		if !r.OK() {
			// The bucket is smaller than the reservation, so no token can
			// ever be handed out. Waiting cannot help; reject at once.
			dec := l.rejectHeadLocked(key, w, ErrRateLimitExceeded, now.Sub(w.enqueued))
			l.mu.Unlock()
			dec.emit(l)
			continue
		}
		delay := r.DelayFrom(now)
		if delay <= 0 {
			dec := l.grantHeadLocked(key, w, now.Sub(w.enqueued))
			l.mu.Unlock()
			dec.emit(l)
			continue
		}
		l.mu.Unlock()

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			l.mu.Lock()
			now := time.Now()
			var dec decision
			switch {
			case w.resolved:
				// The request was canceled while the token was reserved.
				// Return the reservation.
				r.Cancel()
			case !now.Before(w.deadline):
				// The token arrived at the same moment the deadline did.
				// Reject: a request that waited its full max_wait must not
				// be served as if it had been in time.
				r.Cancel()
				dec = l.rejectHeadLocked(key, w, ErrRateLimitTimeout, now.Sub(w.enqueued))
			default:
				dec = l.grantHeadLocked(key, w, now.Sub(w.enqueued))
			}
			l.mu.Unlock()
			dec.emit(l)
		case <-w.done:
			timer.Stop()
			r.Cancel()
		case <-l.closeCh:
			timer.Stop()
			r.Cancel()
			return
		}
	}
}

func (l *RateLimiter) signalLocked() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// peekHeadLocked returns the next identity to serve and its head waiter.
func (l *RateLimiter) peekHeadLocked() (string, *rateWaiter) {
	for len(l.order) > 0 {
		if l.next >= len(l.order) {
			l.next = 0
		}
		key := l.order[l.next]
		q := l.queues[key]
		if q == nil || q.Len() == 0 {
			l.removeKeyLocked(key)
			continue
		}
		return key, q.Front().Value.(*rateWaiter)
	}
	return "", nil
}

func (l *RateLimiter) grantHeadLocked(key string, w *rateWaiter, wait time.Duration) decision {
	l.removeLocked(w)
	if q := l.queues[key]; q != nil && q.Len() > 0 {
		l.next = l.indexLocked(key) + 1
	}
	w.resolved = true
	w.granted = true
	w.wait = wait
	close(w.done)
	l.countAllowedLocked(key, wait, true)
	return l.newDecisionLocked(key, "allowed", wait)
}

func (l *RateLimiter) rejectHeadLocked(key string, w *rateWaiter, err error, wait time.Duration) decision {
	l.removeLocked(w)
	if q := l.queues[key]; q != nil && q.Len() > 0 {
		l.next = l.indexLocked(key) + 1
	}
	l.rejected++
	l.countRejectedLocked(key, wait)
	l.resolveLocked(w, err)
	return l.newDecisionLocked(key, "rejected", wait, "reason", err.Error())
}

// resolve removes w from its queue and resolves it with err unless it has
// already been resolved by the scheduler. It reports whether this call
// resolved the waiter.
func (l *RateLimiter) resolve(w *rateWaiter, err error) bool {
	l.mu.Lock()
	if w.resolved {
		l.mu.Unlock()
		return false
	}
	wait := time.Since(w.enqueued)
	l.rejected++
	l.countRejectedLocked(w.key, wait)
	l.resolveLocked(w, err)
	dec := l.newDecisionLocked(w.key, "rejected", wait, "reason", err.Error())
	l.mu.Unlock()
	dec.emit(l)
	return true
}

// resolveCanceled removes an abandoned request from its queue. Unlike
// resolve, it does not count as a rejection and never touches the token
// bucket.
func (l *RateLimiter) resolveCanceled(w *rateWaiter, err error) bool {
	l.mu.Lock()
	if w.resolved {
		l.mu.Unlock()
		return false
	}
	wait := time.Since(w.enqueued)
	l.canceled++
	l.wait += wait
	c := l.countersLocked(w.key)
	c.wait += wait
	l.resolveLocked(w, err)
	dec := l.newDecisionLocked(w.key, "canceled", wait)
	l.mu.Unlock()
	dec.emit(l)
	return true
}

// resolveLocked resolves w without touching the counters.
func (l *RateLimiter) resolveLocked(w *rateWaiter, err error) {
	l.removeLocked(w)
	w.resolved = true
	w.err = err
	close(w.done)
}

// removeLocked unlinks w from its queue and keeps the round-robin position
// valid.
func (l *RateLimiter) removeLocked(w *rateWaiter) {
	q := l.queues[w.key]
	if q == nil || w.elem == nil {
		return
	}
	wasHead := q.Front() == w.elem
	wasLast := q.Len() == 1
	q.Remove(w.elem)
	w.elem = nil
	l.waiting--
	if wasLast {
		l.removeKeyLocked(w.key)
		return
	}
	if wasHead {
		// The next element moved to the head of this queue.
		l.next = l.indexLocked(w.key)
	}
}

func (l *RateLimiter) removeKeyLocked(key string) {
	idx := l.indexLocked(key)
	if idx < 0 {
		return
	}
	if idx < l.next {
		l.next--
	}
	l.order = append(l.order[:idx], l.order[idx+1:]...)
	delete(l.queues, key)
	if len(l.order) == 0 {
		l.next = 0
	} else if l.next >= len(l.order) {
		l.next = 0
	}
}

func (l *RateLimiter) indexLocked(key string) int {
	for i, k := range l.order {
		if k == key {
			return i
		}
	}
	return -1
}

// waiterResult reads the resolution of w. Closing w.done happens after the
// fields are set, so this is safe without the lock.
func (l *RateLimiter) waiterResult(w *rateWaiter) (time.Duration, error) {
	if w.granted {
		return w.wait, nil
	}
	if w.err != nil {
		return 0, w.err
	}
	return 0, ErrRateLimitExceeded
}

func (l *RateLimiter) countAllowedLocked(identity string, wait time.Duration, delayed bool) {
	l.allowed++
	l.wait += wait
	c := l.countersLocked(identity)
	c.allowed++
	c.wait += wait
	if delayed {
		l.delayed++
		c.delayed++
	}
}

func (l *RateLimiter) countRejectedLocked(identity string, wait time.Duration) {
	l.wait += wait
	c := l.countersLocked(identity)
	c.rejected++
	c.wait += wait
}

func (l *RateLimiter) countCanceled(identity string, wait time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.canceled++
	l.wait += wait
	c := l.countersLocked(identity)
	c.wait += wait
}

func (l *RateLimiter) countersLocked(identity string) *identityCounters {
	c := l.byID[identity]
	if c != nil {
		return c
	}
	if len(l.byID) >= maxTrackedIdentities {
		// The set of identities is attacker-controlled for unauthenticated
		// traffic (one per source address), so cap the map. Drop counters of
		// identities with nothing queued; their aggregate contribution is
		// already in the totals.
		for id := range l.byID {
			if l.queues[id] == nil {
				delete(l.byID, id)
			}
		}
		if len(l.byID) >= maxTrackedIdentities {
			return &identityCounters{}
		}
	}
	c = &identityCounters{}
	l.byID[identity] = c
	return c
}

// decision is a rate limiter outcome waiting to be logged. Decisions are built
// under RateLimiter.mu but emitted after releasing it, so a slow log handler
// cannot block the scheduler or a request.
type decision struct {
	level    slog.Level
	identity string
	outcome  string
	wait     time.Duration
	waiting  int
	extra    []any
}

// newDecisionLocked records a decision. It takes a snapshot of the queue
// length because it is emitted after the lock is released.
func (l *RateLimiter) newDecisionLocked(identity, outcome string, wait time.Duration, extra ...any) decision {
	level := slog.LevelDebug
	switch {
	case outcome == "rejected":
		level = slog.LevelWarn
	case wait > 0, outcome == "canceled":
		level = slog.LevelInfo
	}
	return decision{
		level:    level,
		identity: identity,
		outcome:  outcome,
		wait:     wait,
		waiting:  l.waiting,
		extra:    extra,
	}
}

// emit writes the decision. Identities are "user:<name>" or "ip:<address>";
// no passwords, tokens, or headers are ever logged.
func (d decision) emit(l *RateLimiter) {
	if d.identity == "" {
		return
	}
	if !l.logger.Enabled(context.Background(), d.level) {
		return
	}
	args := []any{
		"identity", d.identity,
		"decision", d.outcome,
		"wait", d.wait,
		"waiting", d.waiting,
	}
	args = append(args, d.extra...)
	l.logMu.Lock()
	l.logger.Log(context.Background(), d.level, "Scrape request rate limiter decision", args...)
	l.logMu.Unlock()
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if host == "" {
		host = "unknown"
	}
	return host
}

// retryAfterSeconds converts a retry delay to the integer-seconds form of the
// Retry-After header.
func retryAfterSeconds(d time.Duration) string {
	seconds := max(int(math.Ceil(d.Seconds())), 1)
	return strconv.Itoa(seconds)
}
