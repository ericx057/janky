package stress

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func postJSON(t *testing.T, client *http.Client, url string, body any) (*http.Response, map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return response, result
}

func getJSON(t *testing.T, client *http.Client, url string) (*http.Response, map[string]any) {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return response, result
}

func field(t *testing.T, object map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := object[key].(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object: %#v", key, object[key])
	}
	return value
}

func waitRun(t *testing.T, client *http.Client, app, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, run := getJSON(t, client, app+"/runs/"+id)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("run status %d: %#v", response.StatusCode, run)
		}
		if run["state"] != "running" {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("run did not finish")
	return nil
}

func TestDirectAgentWorkflowAndRetry(t *testing.T) {
	var mu sync.Mutex
	var events []map[string]any
	var readCalls int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("target body: %v", err)
			return
		}
		mu.Lock()
		events = append(events, event)
		if event["action"] == "read" {
			readCalls++
			if readCalls == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				mu.Unlock()
				return
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	input := map[string]any{
		"target_url": target.URL,
		"profile":    map[string]any{"team": "alpha"},
		"workflow":   []any{map[string]any{"action": "search", "input": map[string]any{"query": "x"}}, map[string]any{"action": "read"}},
		"retries":    1,
	}
	for step := 1; step <= 2; step++ {
		response, result := postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", input)
		if response.StatusCode != http.StatusOK || result["step"] != float64(step) || result["delivered"] != true {
			t.Fatalf("step %d: status %d, result %#v", step, response.StatusCode, result)
		}
		message := field(t, result, "message")
		if message["role"] != "assistant" {
			t.Fatalf("unexpected message: %#v", message)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 || events[0]["action"] != "search" || events[1]["action"] != "read" || events[2]["action"] != "read" {
		t.Fatalf("unexpected target events: %#v", events)
	}
	if events[0]["workflow_id"] != events[1]["workflow_id"] || events[1]["workflow_id"] != events[2]["workflow_id"] {
		t.Fatalf("workflow identity changed: %#v", events)
	}
	if field(t, events[0], "agent")["id"] != "alice" || field(t, field(t, events[0], "agent"), "profile")["team"] != "alpha" {
		t.Fatalf("agent profile missing: %#v", events[0])
	}
	_, status := getJSON(t, app.Client(), app.URL+"/status")
	if field(t, status, "requests")["retries"] != float64(1) {
		t.Fatalf("retry metric missing: %#v", status)
	}
}

func TestFleetConcurrencyStatusAndMetrics(t *testing.T) {
	var mu sync.Mutex
	active, peak := 0, 0
	var events []map[string]any
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("target body: %v", err)
			return
		}
		mu.Lock()
		active++
		if active > peak {
			peak = active
		}
		events = append(events, event)
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		if event["action"] == "review" {
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer target.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	response, created := postJSON(t, app.Client(), app.URL+"/runs", map[string]any{
		"target_url": target.URL, "agents": 8, "concurrency": 3, "arrival_rate": 1000,
		"think_ms": 0, "jitter_ms": 0, "retries": 0,
		"profiles": []any{map[string]any{"team": "a"}, map[string]any{"team": "b"}},
		"workflow": []any{map[string]any{"action": "search"}, map[string]any{"action": "review", "expect_status": []int{403}}},
	})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("create run: status %d, body %#v", response.StatusCode, created)
	}
	id, ok := created["run_id"].(string)
	if !ok || id == "" {
		t.Fatalf("run id missing: %#v", created)
	}
	run := waitRun(t, app.Client(), app.URL, id)
	if run["state"] != "completed" || field(t, run, "agents")["completed"] != float64(8) || field(t, run, "agents")["failed"] != float64(0) {
		t.Fatalf("run did not complete: %#v", run)
	}
	requests := field(t, run, "requests")
	if requests["total"] != float64(16) || requests["expected_non_2xx"] != float64(8) || requests["in_flight"] != float64(0) {
		t.Fatalf("request metrics: %#v", requests)
	}
	mu.Lock()
	if peak > 3 || len(events) != 16 {
		t.Errorf("peak=%d events=%d", peak, len(events))
	}
	mu.Unlock()
	metricResponse, err := app.Client().Get(app.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metricResponse.Body.Close()
	metrics, err := io.ReadAll(metricResponse.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(metrics), "agent_stress_requests_total 16") {
		t.Fatalf("metrics missing total: %s", metrics)
	}
}

func TestMultiScenarioRun(t *testing.T) {
	var mu sync.Mutex
	var events []map[string]any
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("target body: %v", err)
			return
		}
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		if event["action"] == "ticket" {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer target.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	response, created := postJSON(t, app.Client(), app.URL+"/runs", map[string]any{
		"target_url": target.URL, "concurrency": 2, "arrival_rate": 1000, "think_ms": 0, "jitter_ms": 0, "retries": 0,
		"scenarios": []any{
			map[string]any{"name": "sales", "agents": 4, "profiles": []any{map[string]any{"team": "sales", "variant": "a"}, map[string]any{"team": "sales", "variant": "b"}},
				"workflow": []any{map[string]any{"action": "lead"}, map[string]any{"action": "quote"}}},
			map[string]any{"name": "support", "agents": 2, "profiles": []any{map[string]any{"team": "support", "variant": "a"}, map[string]any{"team": "support", "variant": "b"}},
				"workflow": []any{map[string]any{"action": "ticket"}}},
		},
	})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("create run: status %d, body %#v", response.StatusCode, created)
	}
	run := waitRun(t, app.Client(), app.URL, created["run_id"].(string))
	if field(t, run, "agents")["completed"] != float64(4) || field(t, run, "agents")["failed"] != float64(2) || field(t, run, "requests")["total"] != float64(10) {
		t.Fatalf("aggregate counts: %#v", run)
	}
	scenarios := field(t, run, "scenarios")
	if field(t, field(t, scenarios, "sales"), "agents")["completed"] != float64(4) ||
		field(t, field(t, scenarios, "support"), "agents")["failed"] != float64(2) {
		t.Fatalf("scenario counts: %#v", scenarios)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 10 {
		t.Fatalf("events: %#v", events)
	}
	seen := map[string]map[string]int{}
	variants := map[string]map[string]bool{}
	for _, event := range events {
		name := event["scenario"].(string)
		profile := field(t, event, "agent")
		values := field(t, profile, "profile")
		if values["team"] != name {
			t.Fatalf("scenario profile mismatch: %#v", event)
		}
		if variants[name] == nil {
			variants[name] = map[string]bool{}
		}
		variants[name][values["variant"].(string)] = true
		id := profile["id"].(string)
		if seen[id] == nil {
			seen[id] = map[string]int{}
		}
		seen[id][event["action"].(string)]++
	}
	if len(seen) != 6 || len(variants["sales"]) != 2 || len(variants["support"]) != 2 {
		t.Fatalf("agent IDs or profile rotation: ids=%#v variants=%#v", seen, variants)
	}
	for _, actions := range seen {
		if actions["lead"] > 0 && (actions["lead"] != 1 || actions["quote"] != 1) {
			t.Fatalf("sales workflow broken: %#v", actions)
		}
		if actions["ticket"] > 0 && actions["ticket"] != 1 {
			t.Fatalf("support workflow broken: %#v", actions)
		}
	}
}

func TestNamedTargetRouting(t *testing.T) {
	var mu sync.Mutex
	var routes []string
	target := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var event map[string]any
			if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
				t.Errorf("decode %s: %v", name, err)
				return
			}
			mu.Lock()
			routes = append(routes, name+":"+event["action"].(string))
			mu.Unlock()
		}))
	}
	sales := target("sales")
	defer sales.Close()
	support := target("support")
	defer support.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	response, created := postJSON(t, app.Client(), app.URL+"/runs", map[string]any{
		"targets":  map[string]any{"sales": sales.URL, "support": support.URL},
		"think_ms": 0, "jitter_ms": 0, "retries": 0,
		"scenarios": []any{
			map[string]any{"name": "sales", "agents": 1, "target": "sales", "profiles": []any{map[string]any{}}, "workflow": []any{
				map[string]any{"action": "lead"}, map[string]any{"action": "ticket", "target": "support"}}},
			map[string]any{"name": "support", "agents": 1, "target": "support", "profiles": []any{map[string]any{}}, "workflow": []any{
				map[string]any{"action": "reply"}}},
		},
	})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("create run: %d %#v", response.StatusCode, created)
	}
	run := waitRun(t, app.Client(), app.URL, created["run_id"].(string))
	if field(t, run, "agents")["completed"] != float64(2) {
		t.Fatalf("run: %#v", run)
	}
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(routes)
	if !slices.Equal(routes, []string{"sales:lead", "support:reply", "support:ticket"}) {
		t.Fatalf("routes: %#v", routes)
	}
}

func TestRunThresholdsAndTaggedRequests(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("target body: %v", err)
			return
		}
		switch event.Action {
		case "fail":
			w.WriteHeader(http.StatusInternalServerError)
		case "reply":
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer target.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	response, created := postJSON(t, app.Client(), app.URL+"/runs", map[string]any{
		"targets":  map[string]any{"crm": target.URL, "support": target.URL},
		"think_ms": 0, "jitter_ms": 0, "retries": 0,
		"scenarios": []any{
			map[string]any{"name": "sales", "agents": 1, "target": "crm", "profiles": []any{map[string]any{}}, "workflow": []any{
				map[string]any{"action": "lookup"}, map[string]any{"action": "fail", "target": "support"}, map[string]any{"action": "after"}}},
			map[string]any{"name": "support", "agents": 1, "target": "support", "profiles": []any{map[string]any{}}, "workflow": []any{
				map[string]any{"action": "reply", "expect_status": []int{403}}}},
		},
		"thresholds": []any{
			map[string]any{"metric": "failed_rate", "max": 0, "tags": map[string]any{"scenario": "sales", "action": "fail", "target": "support"}},
			map[string]any{"metric": "failed_rate", "max": 0, "tags": map[string]any{"scenario": "support"}},
			map[string]any{"metric": "timeouts", "max": 0, "tags": map[string]any{"action": "after"}},
		},
	})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("create run: %d %#v", response.StatusCode, created)
	}
	run := waitRun(t, app.Client(), app.URL, created["run_id"].(string))
	if run["state"] != "failed" || run["thresholds_passed"] != false || field(t, run, "requests")["total"] != float64(3) {
		t.Fatalf("threshold outcome: %#v", run)
	}
	checks, ok := run["thresholds"].([]any)
	if !ok || len(checks) != 3 || field(t, checks[0].(map[string]any), "tags")["target"] != "support" ||
		checks[0].(map[string]any)["passed"] != false || checks[1].(map[string]any)["passed"] != true ||
		checks[2].(map[string]any)["actual"] != nil {
		t.Fatalf("threshold results: %#v", checks)
	}
	series, ok := run["tagged_requests"].([]any)
	if !ok || len(series) != 3 {
		t.Fatalf("tagged requests: %#v", run["tagged_requests"])
	}
	for _, entry := range series {
		group := entry.(map[string]any)
		tags := field(t, group, "tags")
		if tags["action"] == "fail" && (tags["target"] != "support" || field(t, group, "requests")["failed"] != float64(1)) {
			t.Fatalf("failed request attribution: %#v", group)
		}
		if tags["action"] == "reply" && field(t, group, "requests")["expected_non_2xx"] != float64(1) {
			t.Fatalf("expected response attribution: %#v", group)
		}
	}
}

func TestPassingThresholdKeepsRunCompleted(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	response, created := postJSON(t, app.Client(), app.URL+"/runs", map[string]any{
		"target_url": target.URL, "agents": 1, "jitter_ms": 0,
		"thresholds": []any{map[string]any{"metric": "failed_rate", "max": 0}},
	})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("create run: %d %#v", response.StatusCode, created)
	}
	run := waitRun(t, app.Client(), app.URL, created["run_id"].(string))
	if run["state"] != "completed" || run["thresholds_passed"] != true {
		t.Fatalf("passing threshold: %#v", run)
	}
}

func TestDirectInvokeRejectsNamedTargetSelector(t *testing.T) {
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	response, _ := postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", map[string]any{
		"target_url": "http://127.0.0.1:1234",
		"workflow":   []any{map[string]any{"action": "read", "target": "other"}},
	})
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("direct invoke accepted named target: %d", response.StatusCode)
	}
}

func TestValidationAndRedirectIsolation(t *testing.T) {
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	for _, body := range []map[string]any{
		{"target_url": "file:///tmp/x"},
		{"target_url": "http://example.com"},
		{"target_url": "http://127.0.0.1:9999", "agents": 0},
		{"target_url": "http://127.0.0.1:9999", "concurrency": 0},
		{"target_url": "http://127.0.0.1:9999", "workflow": []any{}},
		{"target_url": "http://user:pass@127.0.0.1:9999"},
	} {
		response, result := postJSON(t, app.Client(), app.URL+"/runs", body)
		if response.StatusCode != http.StatusBadRequest {
			t.Errorf("invalid %#v: status %d, result %#v", body, response.StatusCode, result)
		}
	}
	response, err := app.Client().Post(app.URL+"/runs", "text/plain", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("wrong content type status: %d", response.StatusCode)
	}
	var forwarded atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded.Store(true)
	}))
	defer destination.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer target.Close()
	response, result := postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", map[string]any{"target_url": target.URL})
	if response.StatusCode != http.StatusOK || result["delivered"] != false || forwarded.Load() {
		t.Fatalf("redirect was forwarded: status %d, result %#v, forwarded %v", response.StatusCode, result, forwarded.Load())
	}
}

func TestEventStream(t *testing.T) {
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, app.URL+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := app.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	if stream.StatusCode != http.StatusOK || !strings.Contains(stream.Header.Get("content-type"), "text/event-stream") {
		t.Fatalf("stream: status %d, type %q", stream.StatusCode, stream.Header.Get("content-type"))
	}
	reader := bufio.NewReader(stream.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, "connected") {
		t.Fatalf("stream greeting %q: %v", first, err)
	}
	response, _ := postJSON(t, app.Client(), app.URL+"/agents/observer/invoke", map[string]any{"target_url": target.URL})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("invoke status %d", response.StatusCode)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended before request event: %v", err)
		}
		if strings.Contains(line, `"type":"request"`) {
			break
		}
	}
	response, result := getJSON(t, app.Client(), app.URL+"/runs/00000000-0000-0000-0000-000000000000")
	if response.StatusCode != http.StatusNotFound || result["error"] == nil {
		t.Fatalf("unknown run: status %d, result %#v", response.StatusCode, result)
	}
}

func TestTelemetryDataAndErrors(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer target.Close()
	telemetry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"db":{"connections":12}}`))
		case "/text":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("queue 4"))
		case "/invalid":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{invalid"))
		case "/large":
			_, _ = w.Write(bytes.Repeat([]byte("x"), 16385))
		}
	}))
	defer telemetry.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	for _, item := range []struct {
		path      string
		wantError bool
	}{
		{"/json", false}, {"/text", false}, {"/invalid", true}, {"/large", true},
	} {
		t.Run(item.path, func(t *testing.T) {
			response, created := postJSON(t, app.Client(), app.URL+"/runs", map[string]any{
				"target_url": target.URL, "telemetry_url": telemetry.URL + item.path,
				"agents": 1, "arrival_rate": 1000, "think_ms": 0, "jitter_ms": 0, "retries": 0,
			})
			if response.StatusCode != http.StatusAccepted {
				t.Fatalf("create run: %d, %#v", response.StatusCode, created)
			}
			run := waitRun(t, app.Client(), app.URL, created["run_id"].(string))
			if run["state"] != "completed" || field(t, run, "requests")["total"] != float64(1) {
				t.Fatalf("run failed: %#v", run)
			}
			sample := field(t, run, "target_telemetry")
			if item.wantError {
				if sample["error"] == nil {
					t.Fatalf("missing telemetry error: %#v", sample)
				}
				return
			}
			if sample["status"] != float64(200) {
				t.Fatalf("telemetry status: %#v", sample)
			}
			if item.path == "/json" && field(t, sample["data"].(map[string]any), "db")["connections"] != float64(12) {
				t.Fatalf("JSON telemetry: %#v", sample)
			}
			if item.path == "/text" && sample["data"] != "queue 4" {
				t.Fatalf("text telemetry: %#v", sample)
			}
		})
	}
}

func TestFailedDirectDeliveryKeepsCurrentStep(t *testing.T) {
	var mu sync.Mutex
	var events []map[string]any
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("target body: %v", err)
			return
		}
		mu.Lock()
		events = append(events, event)
		count := len(events)
		mu.Unlock()
		if count == 1 {
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer target.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	input := map[string]any{"target_url": target.URL, "retries": 0,
		"profile":  map[string]any{"team": "alpha"},
		"workflow": []any{map[string]any{"action": "first", "input": map[string]any{"query": "sample"}}, map[string]any{"action": "second"}}}
	first, failed := postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", input)
	second, delivered := postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", map[string]any{"target_url": target.URL})
	third, advanced := postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", map[string]any{"target_url": target.URL})
	if first.StatusCode != 200 || second.StatusCode != 200 || third.StatusCode != 200 ||
		failed["delivered"] != false || delivered["delivered"] != true ||
		failed["step"] != float64(1) || delivered["step"] != float64(1) || advanced["step"] != float64(2) {
		t.Fatalf("progression after failure: %#v %#v %#v", failed, delivered, advanced)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 || events[0]["action"] != "first" || events[1]["action"] != "first" || events[2]["action"] != "second" ||
		events[0]["workflow_id"] != events[1]["workflow_id"] ||
		field(t, events[1], "input")["query"] != "sample" ||
		field(t, field(t, events[1], "agent"), "profile")["team"] != "alpha" {
		t.Fatalf("unexpected retained agent state: %#v", events)
	}
}

func TestBusyAdmission(t *testing.T) {
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
	}))
	defer func() {
		releaseOnce.Do(func() { close(release) })
		target.Close()
	}()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	input := map[string]any{"target_url": target.URL, "agents": 1, "think_ms": 0, "jitter_ms": 0, "retries": 0}
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", input)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("direct call did not reach target")
	}
	if response, _ := postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", input); response.StatusCode != 409 {
		t.Fatalf("same-agent call status %d", response.StatusCode)
	}
	if response, created := postJSON(t, app.Client(), app.URL+"/runs", input); response.StatusCode != 202 || created["run_id"] == nil {
		t.Fatalf("fleet during direct call status %d", response.StatusCode)
	}
	releaseOnce.Do(func() { close(release) })
	<-firstDone

	releaseFleet := make(chan struct{})
	var fleetReleaseOnce sync.Once
	fleetTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-releaseFleet
	}))
	defer func() {
		fleetReleaseOnce.Do(func() { close(releaseFleet) })
		fleetTarget.Close()
	}()
	response, created := postJSON(t, app.Client(), app.URL+"/runs", map[string]any{
		"target_url": fleetTarget.URL, "agents": 1, "timeout_ms": 100,
		"think_ms": 0, "jitter_ms": 0, "retries": 0,
	})
	if response.StatusCode != 202 {
		t.Fatalf("start fleet: %d %#v", response.StatusCode, created)
	}
	if response, _ := postJSON(t, app.Client(), app.URL+"/agents/alice/invoke", input); response.StatusCode != 200 {
		t.Fatalf("direct call during fleet status %d", response.StatusCode)
	}
	other, second := postJSON(t, app.Client(), app.URL+"/runs", map[string]any{
		"target_url": target.URL, "agents": 1, "think_ms": 0, "jitter_ms": 0, "retries": 0,
	})
	if other.StatusCode != 202 || second["run_id"] == created["run_id"] {
		t.Fatalf("second fleet: %d %#v", other.StatusCode, second)
	}
	fleetReleaseOnce.Do(func() { close(releaseFleet) })
	_ = waitRun(t, app.Client(), app.URL, created["run_id"].(string))
	_ = waitRun(t, app.Client(), app.URL, second["run_id"].(string))
}

func TestRetryAfterAndNonRetryableStatus(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]any
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("target body: %v", err)
			return
		}
		action := event["action"].(string)
		mu.Lock()
		counts[action]++
		attempt := counts[action]
		mu.Unlock()
		if attempt > 1 {
			return
		}
		switch action {
		case "seconds":
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(429)
		case "date":
			w.Header().Set("Retry-After", time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat))
			w.WriteHeader(429)
		case "invalid":
			w.Header().Set("Retry-After", "invalid")
			w.WriteHeader(429)
		case "server_error":
			w.WriteHeader(503)
		default:
			w.WriteHeader(400)
		}
	}))
	defer target.Close()
	app := httptest.NewServer(NewServer(nil))
	defer app.Close()
	for _, action := range []string{"seconds", "date", "invalid", "server_error", "client_error"} {
		response, result := postJSON(t, app.Client(), app.URL+"/agents/"+action+"/invoke", map[string]any{
			"target_url": target.URL, "workflow": []any{map[string]any{"action": action}}, "retries": 1,
		})
		want := action != "client_error"
		if response.StatusCode != 200 || result["delivered"] != want {
			t.Fatalf("%s: status %d, result %#v", action, response.StatusCode, result)
		}
	}
	mu.Lock()
	for action, want := range map[string]int{"seconds": 2, "date": 2, "invalid": 2, "server_error": 2, "client_error": 1} {
		if counts[action] != want {
			t.Errorf("%s calls = %d, want %d", action, counts[action], want)
		}
	}
	mu.Unlock()
	_, status := getJSON(t, app.Client(), app.URL+"/status")
	requests := field(t, status, "requests")
	if requests["retries"] != float64(4) || requests["throttled"] != float64(3) || requests["server_errors"] != float64(1) {
		t.Fatalf("retry metrics: %#v", requests)
	}
}

func TestRoutesAndDirectValidation(t *testing.T) {
	s := NewServer(nil).(*server)
	for _, item := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/health", "", 200},
		{"GET", "/status", "", 200},
		{"GET", "/missing", "", 404},
		{"POST", "/agents/invalid!/invoke", "{}", 404},
		{"POST", "/agents/a/invoke", "{", 400},
		{"POST", "/agents/a/invoke", `{"profile":"bad"}`, 400},
		{"POST", "/agents/a/invoke", `{"workflow":[]}`, 400},
		{"POST", "/agents/a/invoke", `{"target_url":"http://example.com"}`, 400},
		{"POST", "/agents/a/invoke", `{"timeout_ms":0}`, 400},
		{"POST", "/agents/a/invoke", `{"timeout_ms":9223372036855}`, 400},
		{"POST", "/agents/a/invoke", `{"retries":-1}`, 400},
		{"POST", "/runs", "{", 400},
	} {
		t.Run(item.method+item.path+item.body, func(t *testing.T) {
			request := httptest.NewRequest(item.method, item.path, strings.NewReader(item.body))
			if item.method == "POST" {
				request.Header.Set("Content-Type", "application/json")
			}
			writer := httptest.NewRecorder()
			s.ServeHTTP(writer, request)
			if writer.Code != item.status {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
		})
	}
	for step := 1; step <= 2; step++ {
		request := httptest.NewRequest("POST", "/agents/a/invoke", strings.NewReader(`{"workflow":[{"action":"one"},{"action":"two"}]}`))
		request.Header.Set("Content-Type", "application/json")
		writer := httptest.NewRecorder()
		s.ServeHTTP(writer, request)
		var result map[string]any
		if err := json.Unmarshal(writer.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if writer.Code != 200 || result["step"] != float64(step) {
			t.Fatalf("local step %d: %s", step, writer.Body.String())
		}
	}
}

func TestAgentStateAndEventObserverLimit(t *testing.T) {
	s := NewServer(nil).(*server)
	for i := 0; i <= 10000; i++ {
		s.saveAgent(fmt.Sprintf("a%d", i), agentState{})
	}
	if len(s.agents) != 10001 || s.agents["a0"].Step != 0 {
		t.Fatalf("agent state was discarded: %d", len(s.agents))
	}
	for i := 0; i < 32; i++ {
		s.listeners[make(chan []byte)] = true
	}
	writer := httptest.NewRecorder()
	s.ServeHTTP(writer, httptest.NewRequest("GET", "/events", nil))
	if writer.Code != 503 {
		t.Fatalf("event observer limit: %d", writer.Code)
	}
	s.emit(map[string]any{"bad": make(chan int)})
	full := make(chan []byte, 1)
	full <- []byte("occupied")
	s.listeners[full] = true
	s.emit(map[string]any{"type": "request"})
	if s.listeners[full] {
		t.Fatal("slow observer was not removed")
	}
}

func TestRunHistoryEviction(t *testing.T) {
	s := NewServer(nil).(*server)
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%d", i)
		s.runs[id] = &runState{ID: id, State: "completed"}
		s.runOrder = append(s.runOrder, id)
	}
	request := httptest.NewRequest("POST", "/runs", strings.NewReader(`{"target_url":"http://127.0.0.1:1","agents":1,"retries":0}`))
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	s.ServeHTTP(writer, request)
	if writer.Code != 202 || len(s.runs) != 100 || s.runs["0"] != nil {
		t.Fatalf("run history eviction: status=%d runs=%d", writer.Code, len(s.runs))
	}
	var created map[string]string
	if err := json.Unmarshal(writer.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		state := s.runs[created["run_id"]].State
		s.mu.Unlock()
		if state == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	writer = httptest.NewRecorder()
	s.ServeHTTP(writer, httptest.NewRequest("GET", "/status", nil))
	if writer.Code != 200 || !strings.Contains(writer.Body.String(), `"runs"`) {
		t.Fatalf("run history status: %s", writer.Body.String())
	}
}

func TestActiveRunsStayQueryableWhenHistoryIsFull(t *testing.T) {
	s := NewServer(nil).(*server)
	for i := 0; i < 101; i++ {
		id := fmt.Sprintf("%d", i)
		s.runs[id] = &runState{ID: id, State: "running"}
		s.runOrder = append(s.runOrder, id)
	}
	s.trimRuns()
	if len(s.runs) != 101 || s.runs["0"] == nil {
		t.Fatalf("active run evicted: %d", len(s.runs))
	}
	s.runs["0"].State = "failed"
	s.trimRuns()
	if len(s.runs) != 100 || s.runs["0"] != nil {
		t.Fatalf("finished run retained: %d", len(s.runs))
	}
}

func TestInvokeUnencodableRetainedProfile(t *testing.T) {
	s := NewServer(nil).(*server)
	s.agents["a"] = agentState{Profile: map[string]any{"bad": make(chan int)}, Workflow: []step{{Action: "read"}}}
	request := httptest.NewRequest("POST", "/agents/a/invoke", strings.NewReader("{}"))
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	s.ServeHTTP(writer, request)
	if writer.Code != 500 {
		t.Fatalf("invalid retained profile: %d", writer.Code)
	}
}

type brokenStreamWriter struct{ *httptest.ResponseRecorder }

func (w brokenStreamWriter) Write(data []byte) (int, error) {
	if bytes.HasPrefix(data, []byte("data: ")) {
		return 0, fmt.Errorf("stream disconnected")
	}
	return w.ResponseRecorder.Write(data)
}

func waitListener(t *testing.T, s *server) chan []byte {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for listener := range s.listeners {
			s.mu.Unlock()
			return listener
		}
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("event listener did not connect")
	return nil
}

func TestEventStreamWriteFailureAndClosedListener(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(fmt.Sprint(broken), func(t *testing.T) {
			s := NewServer(nil).(*server)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			request := httptest.NewRequest("GET", "/events", nil).WithContext(ctx)
			writer := http.ResponseWriter(httptest.NewRecorder())
			if broken {
				writer = brokenStreamWriter{httptest.NewRecorder()}
			}
			done := make(chan struct{})
			go func() { s.ServeHTTP(writer, request); close(done) }()
			listener := waitListener(t, s)
			if broken {
				s.emit(map[string]any{"type": "request"})
			} else {
				s.mu.Lock()
				delete(s.listeners, listener)
				close(listener)
				s.mu.Unlock()
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("event stream did not exit")
			}
		})
	}
}
