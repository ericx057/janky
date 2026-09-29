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

type requestTags struct{ Scenario, Action, Target string }

type weightedLatency struct{ value, weight float64 }

func (tags requestTags) values() map[string]string {
	result := map[string]string{"action": tags.Action}
	if tags.Scenario != "" {
		result["scenario"] = tags.Scenario
	}
	if tags.Target != "" {
		result["target"] = tags.Target
	}
	return result
}

func (m *metrics) record(latency float64, status int, timedOut, expected bool) {
	m.recordBounded(latency, status, timedOut, expected, 10000)
}

func (m *metrics) recordBounded(latency float64, status int, timedOut, expected bool, limit int) {
	m.Total++
	m.LatencySum += latency
	if len(m.Latencies) < limit {
		m.Latencies = append(m.Latencies, latency)
	} else {
		m.Latencies[m.Total%limit] = latency
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

func taggedSnapshot(groups map[requestTags]*metrics) []map[string]any {
	tags := make([]requestTags, 0, len(groups))
	for tag := range groups {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool {
		a, b := tags[i], tags[j]
		if a.Scenario != b.Scenario {
			return a.Scenario < b.Scenario
		}
		if a.Action != b.Action {
			return a.Action < b.Action
		}
		return a.Target < b.Target
	})
	result := make([]map[string]any, 0, len(tags))
	for _, tag := range tags {
		result = append(result, map[string]any{"tags": tag.values(), "requests": groups[tag].snapshot()})
	}
	return result
}

func weightedP95(samples []weightedLatency) float64 {
	sort.Slice(samples, func(i, j int) bool { return samples[i].value < samples[j].value })
	total := 0.0
	for _, sample := range samples {
		total += sample.weight
	}
	cutoff := 0.95 * total
	cumulative := 0.0
	for _, sample := range samples[:len(samples)-1] {
		cumulative += sample.weight
		if cumulative >= cutoff {
			return sample.value
		}
	}
	return samples[len(samples)-1].value
}

func evaluateThresholds(checks []thresholdConfig, groups map[requestTags]*metrics) ([]map[string]any, bool) {
	results := make([]map[string]any, 0, len(checks))
	allPassed := true
	for _, check := range checks {
		var total, failed, timeouts int
		var latencies []weightedLatency
		for tags, group := range groups {
			if check.Scenario != "" && check.Scenario != tags.Scenario ||
				check.Action != "" && check.Action != tags.Action ||
				check.Target != "" && check.Target != tags.Target {
				continue
			}
			total += group.Total
			failed += group.Failed
			timeouts += group.Timeouts
			if check.Metric == "latency_p95_ms" && len(group.Latencies) > 0 {
				weight := float64(group.Total) / float64(len(group.Latencies))
				for _, latency := range group.Latencies {
					latencies = append(latencies, weightedLatency{latency, weight})
				}
			}
		}
		var actual any
		passed := false
		if total > 0 && (check.Metric != "latency_p95_ms" || len(latencies) > 0) {
			value := 0.0
			switch check.Metric {
			case "failed_rate":
				value = float64(failed) / float64(total)
			case "timeouts":
				value = float64(timeouts)
			case "latency_p95_ms":
				value = weightedP95(latencies)
			}
			actual = value
			passed = value <= check.Max
		}
		tags := map[string]string{}
		if check.Scenario != "" {
			tags["scenario"] = check.Scenario
		}
		if check.Action != "" {
			tags["action"] = check.Action
		}
		if check.Target != "" {
			tags["target"] = check.Target
		}
		results = append(results, map[string]any{"metric": check.Metric, "max": check.Max,
			"tags": tags, "actual": actual, "passed": passed})
		allPassed = allPassed && passed
	}
	return results, allPassed
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
