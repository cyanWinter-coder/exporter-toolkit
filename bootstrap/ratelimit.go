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
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/exporter-toolkit/web"
)

// rateLimitCollector publishes the web package's rate limiter counters. It
// reads them on every scrape, so it always reflects the live limiter.
type rateLimitCollector struct {
	server func() *http.Server

	requests *prometheus.Desc
	wait     *prometheus.Desc
	waiting  *prometheus.Desc
	identity *prometheus.Desc
	open     *prometheus.Desc
}

func newRateLimitCollector(server func() *http.Server) *rateLimitCollector {
	const namespace = "exporter_web"
	return &rateLimitCollector{
		server: server,
		requests: prometheus.NewDesc(
			namespace+"_rate_limit_requests_total",
			"Requests through the web rate limiter, by decision.",
			[]string{"decision"}, nil,
		),
		wait: prometheus.NewDesc(
			namespace+"_rate_limit_wait_seconds_total",
			"Total time requests spent waiting in the web rate limiter queue.",
			nil, nil,
		),
		waiting: prometheus.NewDesc(
			namespace+"_rate_limit_waiting_requests",
			"Requests currently waiting in the web rate limiter queue.",
			nil, nil,
		),
		identity: prometheus.NewDesc(
			namespace+"_rate_limit_identity_requests_total",
			"Requests through the web rate limiter for one queue identity, by decision. "+
				"Identities are user names for authenticated users and client addresses otherwise.",
			[]string{"identity", "decision"}, nil,
		),
		open: prometheus.NewDesc(
			namespace+"_rate_limit_identity_open",
			"Whether a queue identity currently has requests waiting.",
			[]string{"identity"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *rateLimitCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.requests
	ch <- c.wait
	ch <- c.waiting
	ch <- c.identity
	ch <- c.open
}

// Collect implements prometheus.Collector. It emits nothing when the server
// has no rate limiter configured, so exporters without one are unaffected.
func (c *rateLimitCollector) Collect(ch chan<- prometheus.Metric) {
	server := c.server()
	if server == nil {
		return
	}
	stats, identities, ok := web.ServerRateLimitStats(server)
	if !ok {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.requests, prometheus.CounterValue, float64(stats.Allowed), "allowed")
	ch <- prometheus.MustNewConstMetric(c.requests, prometheus.CounterValue, float64(stats.Delayed), "delayed")
	ch <- prometheus.MustNewConstMetric(c.requests, prometheus.CounterValue, float64(stats.Rejected), "rejected")
	ch <- prometheus.MustNewConstMetric(c.requests, prometheus.CounterValue, float64(stats.Canceled), "canceled")
	ch <- prometheus.MustNewConstMetric(c.wait, prometheus.CounterValue, stats.TotalWait.Seconds())
	ch <- prometheus.MustNewConstMetric(c.waiting, prometheus.GaugeValue, float64(stats.Waiting))

	for id, s := range identities {
		ch <- prometheus.MustNewConstMetric(c.identity, prometheus.CounterValue, float64(s.Allowed), id, "allowed")
		ch <- prometheus.MustNewConstMetric(c.identity, prometheus.CounterValue, float64(s.Delayed), id, "delayed")
		ch <- prometheus.MustNewConstMetric(c.identity, prometheus.CounterValue, float64(s.Rejected), id, "rejected")
		open := 0.0
		if s.Waiting > 0 {
			open = 1
		}
		ch <- prometheus.MustNewConstMetric(c.open, prometheus.GaugeValue, open, id)
	}
}

var _ prometheus.Collector = (*rateLimitCollector)(nil)
