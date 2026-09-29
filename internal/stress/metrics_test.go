package stress

import (
	"slices"
	"testing"
)

func TestTaggedMetricsAndThresholds(t *testing.T) {
	groups := map[requestTags]*metrics{
		{Scenario: "sales", Action: "lookup", Target: "crm"}:      {Total: 2, Succeeded: 2, Latencies: []float64{10, 20}},
		{Scenario: "sales", Action: "lookup", Target: "support"}:  {Total: 1, Succeeded: 1, Latencies: []float64{15}},
		{Scenario: "sales", Action: "ticket", Target: "support"}:  {Total: 1, Failed: 1, Timeouts: 1, Latencies: []float64{30}},
		{Scenario: "support", Action: "reply", Target: "support"}: {Total: 1, Succeeded: 1, ExpectedNon2xx: 1, Latencies: []float64{40}},
	}
	series := taggedSnapshot(groups)
	if len(series) != 4 || series[0]["tags"].(map[string]string)["scenario"] != "sales" ||
		series[0]["requests"].(map[string]any)["total"] != 2 {
		t.Fatalf("tagged series: %#v", series)
	}
	checks := []thresholdConfig{
		{Metric: "failed_rate", Max: 0, Scenario: "support"},
		{Metric: "failed_rate", Max: 0.2, Scenario: "sales"},
		{Metric: "latency_p95_ms", Max: 40, Target: "support"},
		{Metric: "timeouts", Max: 0, Action: "ticket"},
		{Metric: "timeouts", Max: 0, Action: "unreached"},
	}
	results, passed := evaluateThresholds(checks, groups)
	if passed || len(results) != len(checks) || results[0]["passed"] != true ||
		results[1]["passed"] != false || results[2]["passed"] != true ||
		results[3]["actual"] != float64(1) || results[4]["actual"] != nil || results[4]["passed"] != false {
		t.Fatalf("threshold results: %#v passed=%v", results, passed)
	}
	if empty, ok := evaluateThresholds(nil, groups); !ok || len(empty) != 0 {
		t.Fatalf("empty thresholds: %#v passed=%v", empty, ok)
	}
}

func TestLatencyThresholdWeightsGroupsAndKeepsPrecision(t *testing.T) {
	fast := make([]float64, 1000)
	for index := range fast {
		fast[index] = 10
	}
	slow := make([]float64, 100)
	for index := range slow {
		slow[index] = 1000
	}
	groups := map[requestTags]*metrics{
		{Action: "fast"}: {Total: 100000, Latencies: fast},
		{Action: "slow"}: {Total: 100, Latencies: slow},
	}
	results, passed := evaluateThresholds([]thresholdConfig{{Metric: "latency_p95_ms", Max: 10}}, groups)
	if !passed || results[0]["actual"] != float64(10) {
		t.Fatalf("group sizes skewed p95: %#v", results)
	}
	results, passed = evaluateThresholds([]thresholdConfig{{Metric: "latency_p95_ms", Max: 10}},
		map[requestTags]*metrics{{Action: "precise"}: {Total: 1, Latencies: []float64{10.49}}})
	if passed || results[0]["actual"] != float64(10.49) {
		t.Fatalf("p95 rounded before threshold comparison: %#v", results)
	}
}

func TestTaggedLatencySamplesStayBounded(t *testing.T) {
	var group metrics
	for index := 0; index < 1001; index++ {
		group.recordBounded(float64(index), 200, false, true, 1000)
	}
	if group.Total != 1001 || len(group.Latencies) != 1000 || !slices.Contains(group.Latencies, 1000) {
		t.Fatalf("tagged latency samples: %+v", group)
	}
}

func TestLatencyThresholdRepresentsWholeRunAfterLateSlowdown(t *testing.T) {
	var group metrics
	for index := 0; index < 100000; index++ {
		group.recordBounded(10, 200, false, true, 1000)
	}
	for index := 0; index < 1000; index++ {
		group.recordBounded(1000, 200, false, true, 1000)
	}
	results, passed := evaluateThresholds([]thresholdConfig{{Metric: "latency_p95_ms", Max: 10}},
		map[requestTags]*metrics{{Action: "read"}: &group})
	if !passed || results[0]["actual"] != float64(10) {
		t.Fatalf("late phase dominated whole-run p95: %#v", results)
	}
}
