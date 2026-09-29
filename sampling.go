package main

import (
	"math"
	"runtime"
	"sort"
	"time"
)

type percentileSample struct {
	P50 float64
	P95 float64
	P99 float64
}

// samplePercentiles accepts observations in any unit, such as milliseconds or bytes.
func samplePercentiles(values []float64) percentileSample {
	if len(values) == 0 {
		return percentileSample{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	valueAt := func(percentile float64) float64 {
		return sorted[int(math.Ceil(percentile*float64(len(sorted))))-1]
	}
	return percentileSample{P50: valueAt(0.50), P95: valueAt(0.95), P99: valueAt(0.99)}
}

func sampleLatencyMS(start time.Time) float64 {
	return float64(time.Since(start)) / float64(time.Millisecond)
}

func sampleHeapBytes() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// sampleMemoryPerAgent estimates average incremental heap, not an individual goroutine's memory.
func sampleMemoryPerAgent(baselineHeap, currentHeap uint64, activeAgents int) float64 {
	if activeAgents <= 0 || currentHeap <= baselineHeap {
		return 0
	}
	return float64(currentHeap-baselineHeap) / float64(activeAgents)
}
