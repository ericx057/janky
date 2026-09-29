package stress

import (
	"math"
	"sort"
)

type metrics struct {
	Total, Succeeded, Failed, Throttled, ServerErrors int
	ExpectedNon2xx, ExpectationFailures, Timeouts     int
	Retries, InFlight                                 int
	LatencySum                                        float64
	Latencies                                         []float64
}

func (m *metrics) record(latency float64, status int, timedOut, expected bool) {
	m.Total++
	m.LatencySum += latency
	if len(m.Latencies) < 10000 {
		m.Latencies = append(m.Latencies, latency)
	} else {
		m.Latencies[m.Total%10000] = latency
	}
	if expected {
		m.Succeeded++
	} else {
		m.Failed++
	}
	if expected && (status < 200 || status >= 300) {
		m.ExpectedNon2xx++
	}
	if status != 0 && !expected {
		m.ExpectationFailures++
	}
	if status == 429 {
		m.Throttled++
	}
	if status >= 500 {
		m.ServerErrors++
	}
	if timedOut {
		m.Timeouts++
	}
}

func (m *metrics) snapshot() map[string]any {
	latencies := append([]float64(nil), m.Latencies...)
	sort.Float64s(latencies)
	p95 := 0
	if len(latencies) > 0 {
		p95 = int(math.Round(latencies[int(math.Ceil(float64(len(latencies))*0.95))-1]))
	}
	average := 0
	if m.Total > 0 {
		average = int(math.Round(m.LatencySum / float64(m.Total)))
	}
	return map[string]any{
		"total": m.Total, "succeeded": m.Succeeded, "failed": m.Failed,
		"throttled": m.Throttled, "server_errors": m.ServerErrors,
		"expected_non_2xx": m.ExpectedNon2xx, "expectation_failures": m.ExpectationFailures,
		"timeouts": m.Timeouts, "retries": m.Retries, "in_flight": m.InFlight,
		"latency_avg_ms": average, "latency_p95_ms": p95,
	}
}
