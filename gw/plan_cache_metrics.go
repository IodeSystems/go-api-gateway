package gateway

import (
	"github.com/IodeSystems/graphql-go/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// planCacheCollector exposes PlanCache.HitsMisses() as Prometheus
// counters via a custom Collector — atomic counters in the cache are
// read at scrape time, so there's no goroutine, no double-counting,
// and no value drift between the cache and the metric.
//
// Two counters:
//   - go_api_gateway_plan_cache_hits_total: lookups that returned a
//     pre-built *Plan (skipped parse + validate + plan).
//   - go_api_gateway_plan_cache_misses_total: lookups that paid the
//     parse + validate + plan cost. A growing miss rate at steady
//     load means the working set is exceeding MaxEntries; consider
//     WithDocCacheSize.
//
// Hit ratio comes out of `rate(hits) / (rate(hits) + rate(misses))`
// in PromQL — we don't expose a precomputed gauge to keep the metric
// surface boring.
type planCacheCollector struct {
	cache  *graphql.PlanCache
	hits   *prometheus.Desc
	misses *prometheus.Desc
}

func newPlanCacheCollector(cache *graphql.PlanCache) *planCacheCollector {
	return &planCacheCollector{
		cache: cache,
		hits: prometheus.NewDesc(
			"go_api_gateway_plan_cache_hits_total",
			"Total parsed+validated+planned-query lookups that hit the plan cache (skipped parse + validate + plan).",
			nil, nil,
		),
		misses: prometheus.NewDesc(
			"go_api_gateway_plan_cache_misses_total",
			"Total parsed+validated+planned-query lookups that missed the cache and paid parse + validate + plan.",
			nil, nil,
		),
	}
}

func (c *planCacheCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.hits
	ch <- c.misses
}

func (c *planCacheCollector) Collect(ch chan<- prometheus.Metric) {
	h, m := c.cache.HitsMisses()
	ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(h))
	ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(m))
}

// unwrapPrometheusMetrics digs through the statsRecordingMetrics
// wrapper to find the underlying *prometheusMetrics. Returns nil
// when the operator passed WithMetrics(custom) — those callers own
// their own collectors anyway.
func unwrapPrometheusMetrics(m Metrics) *prometheusMetrics {
	if w, ok := m.(*statsRecordingMetrics); ok {
		m = w.Metrics
	}
	pm, _ := m.(*prometheusMetrics)
	return pm
}
