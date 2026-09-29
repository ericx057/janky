package stress

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestMetricsBoundedSamplesAndTimeout(t *testing.T) {
	var m metrics
	for i := 0; i < 10001; i++ {
		m.record(float64(i), 0, i == 10000, false)
	}
	if len(m.Latencies) != 10000 || m.Latencies[1] != 10000 || m.Timeouts != 1 || m.Total != 10001 {
		t.Fatalf("bounded samples or timeout count: %+v", m)
	}
}

func TestExpectedStatusAndRetryDelayEdges(t *testing.T) {
	if !expectedStatus(step{}, 204) || expectedStatus(step{}, 400) ||
		!expectedStatus(step{ExpectStatus: []int{202, 409}}, 409) || expectedStatus(step{ExpectStatus: []int{202}}, 500) {
		t.Fatal("unexpected status matching")
	}
	response := &http.Response{Header: make(http.Header)}
	response.Header.Set("Retry-After", "9")
	if retryDelay(response, 0) != time.Second {
		t.Fatal("numeric retry delay was not capped")
	}
	response.Header.Set("Retry-After", time.Now().Add(3*time.Second).UTC().Format(http.TimeFormat))
	if retryDelay(response, 0) != time.Second {
		t.Fatal("date retry delay was not capped")
	}
	response.Header.Set("Retry-After", time.Now().Add(1*time.Second).UTC().Format(http.TimeFormat))
	if delay := retryDelay(response, 0); delay <= 0 || delay > time.Second {
		t.Fatalf("unexpected date retry delay: %s", delay)
	}
	if retryDelay(nil, 1) != 100*time.Millisecond {
		t.Fatal("fallback retry delay")
	}
}

func TestDispatchTransportFailuresAndTimeout(t *testing.T) {
	for _, item := range []struct {
		name    string
		trip    roundTripFunc
		timeout bool
	}{
		{"network", func(*http.Request) (*http.Response, error) { return nil, errors.New("dial failed") }, false},
		{"timeout", func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() }, true},
		{"body", func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(errorReader{}), Header: make(http.Header)}, nil
		}, false},
	} {
		t.Run(item.name, func(t *testing.T) {
			s := NewServer(nil).(*server)
			s.client.Transport = item.trip
			result := s.dispatch("http://localhost/", eventFor("a", nil, 0, "w", []step{{Action: "read"}}), step{Action: "read"}, 1, 0, nil)
			if result || s.global.Total != 1 || s.global.Timeouts != btoi(item.timeout) {
				t.Fatalf("dispatch result=%v metrics=%+v", result, s.global)
			}
		})
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestTelemetryTransportAndBodyErrors(t *testing.T) {
	for _, item := range []struct {
		name, want string
		trip       roundTripFunc
	}{
		{"transport", "telemetry request failed", func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") }},
		{"body", "telemetry request failed", func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(errorReader{}), Header: make(http.Header)}, nil
		}},
		{"timeout", "telemetry request timed out", func(r *http.Request) (*http.Response, error) {
			return nil, context.DeadlineExceeded
		}},
	} {
		t.Run(item.name, func(t *testing.T) {
			s := NewServer(nil).(*server)
			s.client.Transport = item.trip
			run := &runState{}
			s.sampleTelemetry(run, "http://localhost/")
			value := run.Telemetry.(map[string]any)
			if value["error"] != item.want {
				t.Fatalf("telemetry: %#v", value)
			}
		})
	}
}

func TestDispatchRejectsUnencodableEvent(t *testing.T) {
	s := NewServer(nil).(*server)
	event := eventFor("a", nil, 0, "w", []step{{Action: "read", Input: map[string]any{"bad": make(chan int)}}})
	if s.dispatch("http://localhost/", event, step{Action: "read"}, 100, 0, nil) || s.global.Total != 0 {
		t.Fatal("invalid event reached dispatch")
	}
}

func TestFleetDispatchCountsRetries(t *testing.T) {
	s := NewServer(nil).(*server)
	attempts := 0
	s.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		status := 200
		if attempts == 1 {
			status = 429
		}
		return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"0"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	run := &runState{ID: "r"}
	event := eventFor("a", nil, 0, "w", []step{{Action: "read"}})
	if !s.dispatch("http://localhost/", event, step{Action: "read"}, 100, 1, run) || attempts != 2 || run.Metrics.Retries != 1 {
		t.Fatalf("fleet retry attempts=%d metrics=%+v", attempts, run.Metrics)
	}
}

func TestRunAgentFailedDelivery(t *testing.T) {
	s := NewServer(nil).(*server)
	s.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})
	run := &runState{ID: "r"}
	s.runAgent(run, runConfig{TargetURL: "http://localhost/", Profiles: []map[string]any{{}}, Workflow: []step{{Action: "fail"}}, TimeoutMS: 100, Retries: 0}, 0)
	if run.Agents.Started != 1 || run.Agents.Failed != 1 || run.Agents.Completed != 0 || s.totals.Failed != 1 {
		t.Fatalf("failed agent accounting: %+v", run.Agents)
	}
}

func TestFleetSamplesTelemetryWhileRunning(t *testing.T) {
	s := NewServer(nil).(*server)
	var samples atomic.Int32
	s.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			samples.Add(1)
		} else {
			time.Sleep(1100 * time.Millisecond)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}, nil
	})
	run := &runState{ID: "r"}
	s.runFleet(run, runConfig{TargetURL: "http://localhost/", TelemetryURL: "http://localhost/telemetry", Profiles: []map[string]any{{}}, Workflow: []step{{Action: "read"}}, Agents: 1, Concurrency: 1, ArrivalRate: 1000, TimeoutMS: 2000, Retries: 0})
	if run.State != "completed" || samples.Load() < 2 {
		t.Fatalf("telemetry samples=%d run=%+v", samples.Load(), run)
	}
}
