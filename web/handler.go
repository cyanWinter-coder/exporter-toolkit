// Copyright 2020 The Prometheus Authors
// This code is partly borrowed from Caddy:
//    Copyright 2015 Matthew Holt and The Caddy Authors
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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/crypto/bcrypt"
)

// extraHTTPHeaders is a map of HTTP headers that can be added to HTTP
// responses.
// This is private on purpose to ensure consistency in the Prometheus ecosystem.
var extraHTTPHeaders = map[string][]string{
	"Strict-Transport-Security": nil,
	"X-Content-Type-Options":    {"nosniff"},
	"X-Frame-Options":           {"deny", "sameorigin"},
	"X-XSS-Protection":          nil,
	"Content-Security-Policy":   nil,
}

func validateUsers(configPath string) error {
	c, err := getConfig(configPath)
	if err != nil {
		return err
	}

	for _, p := range c.Users {
		_, err = bcrypt.Cost([]byte(p))
		if err != nil {
			return err
		}
	}

	return nil
}

// validateHeaderConfig checks that the provided header configuration is correct.
// It does not check the validity of all the values, only the ones which are
// well-defined enumerations.
func validateHeaderConfig(headers map[string]string) error {
HeadersLoop:
	for k, v := range headers {
		values, ok := extraHTTPHeaders[k]
		if !ok {
			return fmt.Errorf("HTTP header %q can not be configured", k)
		}
		for _, allowedValue := range values {
			if v == allowedValue {
				continue HeadersLoop
			}
		}
		if len(values) > 0 {
			return fmt.Errorf("invalid value for %s. Expected one of: %q, but got: %q", k, values, v)
		}
	}
	return nil
}

type webHandler struct {
	tlsConfigPath string
	handler       http.Handler
	logger        *slog.Logger
	cache         *cache
	rateLimiter   *RateLimiter
	// bcryptMtx is there to ensure that bcrypt.CompareHashAndPassword is run
	// only once in parallel as this is CPU intensive.
	bcryptMtx sync.Mutex
}

func (u *webHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := getConfig(u.tlsConfigPath)
	if err != nil {
		u.logger.Error("Unable to parse configuration", "err", err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Configure http headers.
	for k, v := range c.HTTPConfig.Header {
		w.Header().Set(k, v)
	}

	// Requests are queued by the principal they claim when that principal is a
	// configured user, and by client address otherwise. The lookup is a map
	// lookup rather than a password check, so the limiter runs before bcrypt:
	// a flood of requests cannot burn CPU hashing. The set of user queues is
	// bounded by the configuration, so callers cannot create a queue each by
	// inventing names, and a request that names a real user shares that
	// user's queue even if its password is wrong.
	identity := "ip:" + clientIP(r)
	if len(c.Users) > 0 {
		if user, _, ok := r.BasicAuth(); ok {
			if _, known := c.Users[user]; known {
				identity = "user:" + user
			}
		}
	}

	// The limiter also applies to requests that will be answered with 401, so
	// that unauthenticated traffic cannot be used to bypass the shared bucket.
	if !u.admit(w, r, identity) {
		return
	}

	authorized := len(c.Users) == 0
	if !authorized {
		if user, pass, ok := r.BasicAuth(); ok {
			authorized = u.validCredentials(c, user, pass)
		}
	}

	if !authorized {
		w.Header().Set("WWW-Authenticate", "Basic")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	u.handler.ServeHTTP(w, r)
}

// validCredentials reports whether user and pass match the configured basic
// auth users. Password checks are cached, and unknown users are compared
// against a fixed hash to keep request timing uninformative.
func (u *webHandler) validCredentials(c *Config, user, pass string) bool {
	hashedPassword, validUser := c.Users[user]

	if !validUser {
		// The user is not found. Use a fixed password hash to
		// prevent user enumeration by timing requests.
		// This is a bcrypt-hashed version of "fakepassword".
		hashedPassword = "$2y$10$QOauhQNbBCuQDKes6eFzPeMqBSjb7Mr5DUmpZ/VcEd00UAV/LDeSi"
	}

	cacheKey := strings.Join(
		[]string{
			hex.EncodeToString([]byte(user)),
			hex.EncodeToString([]byte(hashedPassword)),
			hex.EncodeToString([]byte(pass)),
		}, ":")
	authOk, ok := u.cache.get(cacheKey)

	if !ok {
		// This user, hashedPassword, password is not cached.
		u.bcryptMtx.Lock()
		err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(pass))
		u.bcryptMtx.Unlock()

		authOk = validUser && err == nil
		u.cache.set(cacheKey, authOk)
	}

	return authOk && validUser
}

// rateLimitStats is implemented by handlers with a rate limiter. It keeps the
// exported accessor below from depending on the concrete handler type.
type rateLimitStats interface {
	rateLimitSnapshot() (RateLimitStats, map[string]IdentityStats, bool)
}

func (u *webHandler) rateLimitSnapshot() (RateLimitStats, map[string]IdentityStats, bool) {
	if u.rateLimiter == nil {
		return RateLimitStats{}, nil, false
	}
	return u.rateLimiter.Stats(), u.rateLimiter.IdentityStats(), true
}

// ServerRateLimitStats returns the rate limiter counters for server. The
// second return value holds per-identity counters, keyed by "user:<name>" or
// "ip:<address>". It returns false when server has no rate limiter configured.
//
// This lets exporters publish the counters as their own metrics (for example
// exporter_web_rate_limit_allowed_total) without the web package depending on
// a metrics library.
func ServerRateLimitStats(server *http.Server) (RateLimitStats, map[string]IdentityStats, bool) {
	if server == nil {
		return RateLimitStats{}, nil, false
	}
	s, ok := server.Handler.(rateLimitStats)
	if !ok {
		return RateLimitStats{}, nil, false
	}
	return s.rateLimitSnapshot()
}

// admit applies the configured rate limiter. It writes the rejection response
// and returns false when the request must not be served.
func (u *webHandler) admit(w http.ResponseWriter, r *http.Request, identity string) bool {
	limiter := u.rateLimiter
	if limiter == nil || limiter.Exempt(r.URL.Path) {
		return true
	}

	if _, err := limiter.Acquire(r.Context(), identity); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// The scrape went away; there is nobody left to answer.
			return false
		}
		status := http.StatusTooManyRequests
		if errors.Is(err, ErrRateLimiterClosed) {
			// The server is shutting down. 503 tells clients to retry
			// rather than that they were too fast.
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Retry-After", retryAfterSeconds(limiter.RetryAfter()))
		http.Error(w, http.StatusText(status), status)
		return false
	}
	return true
}
