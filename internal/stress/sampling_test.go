package stress

import (
	"testing"
	"time"
)

func TestSamplePercentiles(t *testing.T) {
	if got := samplePercentiles(nil); got != (percentileSample{}) {
		t.Fatalf("empty sample: %+v", got)
	}
	values := []float64{100, 1, 95, 50, 99}
	if got := samplePercentiles(values); got != (percentileSample{P50: 95, P95: 100, P99: 100}) {
		t.Fatalf("percentiles: %+v", got)
	}
	if values[0] != 100 {
		t.Fatal("samples were mutated")
	}
}

func TestSampleMemoryAndLatency(t *testing.T) {
	_ = sampleHeapBytes()
	if got := sampleMemoryPerAgent(100, 500, 4); got != 100 {
		t.Fatalf("bytes per agent: %v", got)
	}
	if sampleMemoryPerAgent(500, 100, 4) != 0 || sampleMemoryPerAgent(100, 500, 0) != 0 {
		t.Fatal("invalid memory sample should be zero")
	}
	if sampleLatencyMS(time.Now().Add(-time.Millisecond)) <= 0 {
		t.Fatal("latency should be positive")
	}
}
