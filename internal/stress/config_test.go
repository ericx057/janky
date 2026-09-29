package stress

import (
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestReadJSON(t *testing.T) {
	for _, tc := range []struct {
		name, contentType, body string
		readError               bool
		wantError               bool
	}{
		{"valid", " Application/JSON; charset=utf-8", `{"n":1}`, false, false},
		{"wrong type", "text/plain", `{}`, false, true},
		{"read error", "application/json", ``, true, true},
		{"too large", "application/json", strings.Repeat("x", 65537), false, true},
		{"invalid", "application/json", `{`, false, true},
		{"null", "application/json", `null`, false, true},
		{"array", "application/json", `[]`, false, true},
		{"trailing value", "application/json", `{} {}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
			if tc.readError {
				r.Body = io.NopCloser(failingReader{})
			}
			r.Header.Set("Content-Type", tc.contentType)
			body, err := readJSON(r)
			if (err != nil) != tc.wantError {
				t.Fatalf("body=%v err=%v", body, err)
			}
			if !tc.wantError && body["n"] != json.Number("1") {
				t.Fatalf("number was not preserved: %#v", body)
			}
		})
	}
}

func TestNumber(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   any
		want    int
		invalid bool
	}{
		{"missing", nil, 7, false},
		{"integer", json.Number("3"), 3, false},
		{"wrong type", 3, 0, true},
		{"malformed", json.Number("not-a-number"), 0, true},
		{"NaN", json.Number("NaN"), 0, true},
		{"fraction", json.Number("1.5"), 0, true},
		{"below", json.Number("0"), 0, true},
		{"above former cap", json.Number("11"), 11, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := number(map[string]any{"value": tc.value}, "value", 7, 1)
			if n != tc.want || (err != nil) != tc.invalid {
				t.Fatalf("number=%d err=%v", n, err)
			}
		})
	}
}

func TestParseProfiles(t *testing.T) {
	for _, value := range []any{nil, []any{}, []any{"bad"}} {
		if _, err := parseProfiles(value); err == nil {
			t.Errorf("accepted profiles %#v", value)
		}
	}
	profiles, err := parseProfiles([]any{map[string]any{"id": "a"}})
	if err != nil || len(profiles) != 1 || profiles[0]["id"] != "a" {
		t.Fatalf("profiles=%#v err=%v", profiles, err)
	}
	large := make([]any, 101)
	for i := range large {
		large[i] = map[string]any{"id": i}
	}
	if profiles, err := parseProfiles(large); err != nil || len(profiles) != len(large) {
		t.Fatalf("large profiles=%d err=%v", len(profiles), err)
	}
}

func TestParseWorkflow(t *testing.T) {
	for _, value := range []any{
		nil, []any{}, []any{"bad"},
		[]any{map[string]any{}},
		[]any{map[string]any{"action": "bad space"}},
		[]any{map[string]any{"action": "ok", "input": []any{}}},
		[]any{map[string]any{"action": "ok", "expect_status": "200"}},
		[]any{map[string]any{"action": "ok", "expect_status": []any{}}},
		[]any{map[string]any{"action": "ok", "expect_status": []any{200}}},
		[]any{map[string]any{"action": "ok", "expect_status": []any{json.Number("bad")}}},
		[]any{map[string]any{"action": "ok", "expect_status": []any{json.Number("200.5")}}},
		[]any{map[string]any{"action": "ok", "expect_status": []any{json.Number("99")}}},
		[]any{map[string]any{"action": "ok", "expect_status": []any{json.Number("600")}}},
	} {
		if _, err := parseWorkflow(value); err == nil {
			t.Errorf("accepted workflow %#v", value)
		}
	}
	steps, err := parseWorkflow([]any{map[string]any{
		"action": "read.v1", "input": map[string]any{"query": "x"},
		"expect_status": []any{json.Number("200"), json.Number("404")},
	}, map[string]any{"action": "next"}})
	if err != nil || len(steps) != 2 || steps[0].Action != "read.v1" ||
		steps[0].Input["query"] != "x" || len(steps[0].ExpectStatus) != 2 ||
		steps[1].Action != "next" || len(steps[1].Input) != 0 {
		t.Fatalf("steps=%#v err=%v", steps, err)
	}
	large := make([]any, 33)
	for i := range large {
		large[i] = map[string]any{"action": "ok"}
	}
	if steps, err := parseWorkflow(large); err != nil || len(steps) != len(large) {
		t.Fatalf("large workflow=%d err=%v", len(steps), err)
	}
}

func TestTargetURL(t *testing.T) {
	allowed := map[string]bool{"example.com": true}
	for _, value := range []any{nil, "%", "file://example.com/", "http://other.com/", "http://user:pass@example.com/", "http://example.com/#frag", "http:///path"} {
		if _, err := targetURL(value, allowed); err == nil {
			t.Errorf("accepted target URL %#v", value)
		}
	}
	url, err := targetURL("https://EXAMPLE.COM/path?q=1", allowed)
	if err != nil || url != "https://EXAMPLE.COM/path?q=1" {
		t.Fatalf("url=%q err=%v", url, err)
	}
}

func TestParseRun(t *testing.T) {
	allowed := map[string]bool{"example.com": true}
	base := map[string]any{"target_url": "http://example.com"}
	config, err := parseRun(base, allowed)
	if err != nil || config.Agents != 1 || config.Workers != 1 || len(config.Profiles) != 1 ||
		len(config.Workflow) != 1 || config.Workflow[0].Action != "request" {
		t.Fatalf("default config=%#v err=%v", config, err)
	}
	for _, tc := range []struct {
		key   string
		value any
	}{
		{"target_url", "file:///tmp"}, {"telemetry_url", "file:///tmp"},
		{"profiles", []any{}}, {"workflow", []any{}},
		{"agents", json.Number("0")}, {"concurrency", json.Number("0")},
		{"arrival_rate", json.Number("0")}, {"think_ms", json.Number("-1")},
		{"jitter_ms", json.Number("-1")}, {"timeout_ms", json.Number("0")},
		{"timeout_ms", json.Number("9223372036855")},
		{"retries", json.Number("-1")},
		{"seed", json.Number("-1")},
		{"workers", json.Number("0")},
	} {
		t.Run(tc.key, func(t *testing.T) {
			body := map[string]any{"target_url": base["target_url"], tc.key: tc.value}
			if _, err := parseRun(body, allowed); err == nil {
				t.Fatalf("accepted invalid %s=%v", tc.key, tc.value)
			}
		})
	}
	config, err = parseRun(map[string]any{
		"target_url": "http://example.com", "telemetry_url": "https://example.com/metrics",
		"agents": json.Number("1000"), "workers": json.Number("2"),
	}, allowed)
	if err != nil || config.TelemetryURL != "https://example.com/metrics" || config.Workers != 2 {
		t.Fatalf("explicit config=%#v err=%v", config, err)
	}
	config, err = parseRun(map[string]any{"target_url": "http://example.com", "agents": json.Number("1000")}, allowed)
	if err != nil || config.Workers < 1 || config.Workers > 4 {
		t.Fatalf("worker default=%#v err=%v", config, err)
	}
	config, err = parseRun(map[string]any{
		"target_url": "http://example.com", "agents": json.Number("10001"),
		"concurrency": json.Number("257"), "arrival_rate": json.Number("10001"),
		"retries": json.Number("6"), "workers": json.Number("9"),
	}, allowed)
	if err != nil || config.Agents != 10001 || config.Concurrency != 257 || config.ArrivalRate != 10001 || config.Retries != 6 || config.Workers != 9 {
		t.Fatalf("large config=%#v err=%v", config, err)
	}
}

func TestParseRunScenarios(t *testing.T) {
	allowed := map[string]bool{"example.com": true}
	item := func(name string) map[string]any {
		return map[string]any{"name": name, "agents": json.Number("2"),
			"profiles": []any{map[string]any{"team": name}},
			"workflow": []any{map[string]any{"action": "read"}}}
	}
	config, err := parseRun(map[string]any{"target_url": "http://example.com", "scenarios": []any{item("sales"), item("support")}}, allowed)
	if err != nil || len(config.Scenarios) != 2 || config.Scenarios[0].Name != "sales" ||
		config.Scenarios[1].Agents != 2 || config.Scenarios[1].Profiles[0]["team"] != "support" {
		t.Fatalf("scenarios=%#v err=%v", config.Scenarios, err)
	}
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"empty", []any{}}, {"wrong type", "sales"}, {"bad entry", []any{"sales"}},
		{"missing name", []any{map[string]any{"agents": json.Number("1"), "profiles": item("x")["profiles"], "workflow": item("x")["workflow"]}}},
		{"invalid name", []any{item("bad name")}}, {"duplicate name", []any{item("sales"), item("sales")}},
		{"missing agents", []any{map[string]any{"name": "sales", "profiles": item("x")["profiles"], "workflow": item("x")["workflow"]}}},
		{"bad agents", []any{map[string]any{"name": "sales", "agents": json.Number("0"), "profiles": item("x")["profiles"], "workflow": item("x")["workflow"]}}},
		{"missing profiles", []any{map[string]any{"name": "sales", "agents": json.Number("1"), "workflow": item("x")["workflow"]}}},
		{"missing workflow", []any{map[string]any{"name": "sales", "agents": json.Number("1"), "profiles": item("x")["profiles"]}}},
		{"bad profiles", []any{map[string]any{"name": "sales", "agents": json.Number("1"), "profiles": []any{}, "workflow": item("x")["workflow"]}}},
		{"bad workflow", []any{map[string]any{"name": "sales", "agents": json.Number("1"), "profiles": item("x")["profiles"], "workflow": []any{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseRun(map[string]any{"target_url": "http://example.com", "scenarios": tc.value}, allowed); err == nil {
				t.Fatal("accepted invalid scenarios")
			}
		})
	}
	for _, key := range []string{"agents", "profiles", "workflow"} {
		if _, err := parseRun(map[string]any{"target_url": "http://example.com", "scenarios": []any{item("sales")}, key: item("sales")[key]}, allowed); err == nil {
			t.Fatalf("accepted mixed %s and scenarios", key)
		}
	}
	large := item("large")
	large["agents"] = json.Number(strconv.Itoa(int(^uint(0) >> 1)))
	if _, err := parseRun(map[string]any{"target_url": "http://example.com", "scenarios": []any{large, item("extra")}}, allowed); err == nil {
		t.Fatal("accepted scenario agent total overflow")
	}
}

func TestParseScenarioPacing(t *testing.T) {
	allowed := map[string]bool{"example.com": true}
	item := map[string]any{"name": "sales", "agents": json.Number("2"), "profiles": []any{map[string]any{}},
		"workflow": []any{map[string]any{"action": "read"}}, "arrival_rate": json.Number("25"), "concurrency": json.Number("2")}
	config, err := parseRun(map[string]any{"target_url": "http://example.com", "arrival_rate": json.Number("10"),
		"concurrency": json.Number("4"), "scenarios": []any{item}}, allowed)
	if err != nil || config.Scenarios[0].ArrivalRate != 25 || config.Scenarios[0].Concurrency != 2 {
		t.Fatalf("scenario pacing: %#v err=%v", config.Scenarios, err)
	}
	for _, key := range []string{"arrival_rate", "concurrency"} {
		for _, value := range []any{json.Number("0"), json.Number("-1"), "bad"} {
			invalid := map[string]any{"name": "sales", "agents": json.Number("1"), "profiles": item["profiles"], "workflow": item["workflow"], key: value}
			if _, err := parseRun(map[string]any{"target_url": "http://example.com", "scenarios": []any{invalid}}, allowed); err == nil {
				t.Fatalf("accepted %s=%v", key, value)
			}
		}
	}
	item = map[string]any{"name": "sales", "agents": json.Number("1"), "profiles": item["profiles"], "workflow": item["workflow"]}
	config, err = parseRun(map[string]any{"target_url": "http://example.com", "scenarios": []any{item}}, allowed)
	if err != nil || config.Scenarios[0].ArrivalRate != 0 || config.Scenarios[0].Concurrency != 0 {
		t.Fatalf("legacy pacing changed: %#v err=%v", config.Scenarios, err)
	}
}

func TestParseRunNamedTargets(t *testing.T) {
	allowed := map[string]bool{"example.com": true}
	base := map[string]any{
		"targets": map[string]any{"sales": "https://example.com/sales", "support": "https://example.com/support"},
		"scenarios": []any{
			map[string]any{"name": "sales", "agents": json.Number("1"), "profiles": []any{map[string]any{}},
				"target": "sales", "workflow": []any{map[string]any{"action": "lead"}, map[string]any{"action": "ticket", "target": "support"}}},
		},
	}
	config, err := parseRun(base, allowed)
	if err != nil || config.Scenarios[0].TargetURL != "https://example.com/sales" ||
		config.Scenarios[0].Workflow[1].TargetURL != "https://example.com/support" {
		t.Fatalf("named targets: %#v err=%v", config, err)
	}
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"missing fallback", map[string]any{"targets": map[string]any{"one": "https://example.com/"}}},
		{"bad targets type", map[string]any{"target_url": "https://example.com/", "targets": []any{}}},
		{"empty targets", map[string]any{"target_url": "https://example.com/", "targets": map[string]any{}}},
		{"bad name", map[string]any{"target_url": "https://example.com/", "targets": map[string]any{"bad name": "https://example.com/"}}},
		{"bad URL", map[string]any{"target_url": "https://example.com/", "targets": map[string]any{"one": "file:///tmp"}}},
		{"disallowed host", map[string]any{"target_url": "https://example.com/", "targets": map[string]any{"one": "https://other.com/"}}},
		{"unknown scenario target", map[string]any{"targets": map[string]any{"one": "https://example.com/"}, "scenarios": []any{
			map[string]any{"name": "s", "agents": json.Number("1"), "profiles": []any{map[string]any{}}, "target": "missing", "workflow": []any{map[string]any{"action": "a"}}}}}},
		{"unresolved scenario step", map[string]any{"targets": map[string]any{"one": "https://example.com/"}, "scenarios": []any{
			map[string]any{"name": "s", "agents": json.Number("1"), "profiles": []any{map[string]any{}}, "workflow": []any{map[string]any{"action": "a"}}}}}},
		{"bad scenario target", map[string]any{"target_url": "https://example.com/", "scenarios": []any{
			map[string]any{"name": "s", "agents": json.Number("1"), "profiles": []any{map[string]any{}}, "target": 1, "workflow": []any{map[string]any{"action": "a"}}}}}},
		{"unknown step target", map[string]any{"target_url": "https://example.com/", "workflow": []any{map[string]any{"action": "a", "target": "missing"}}}},
		{"bad step target", map[string]any{"target_url": "https://example.com/", "workflow": []any{map[string]any{"action": "a", "target": 1}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseRun(tc.body, allowed); err == nil {
				t.Fatal("accepted invalid target configuration")
			}
		})
	}
	config, err = parseRun(map[string]any{"target_url": "https://example.com/default", "targets": map[string]any{"one": "https://example.com/one"},
		"workflow": []any{map[string]any{"action": "first"}, map[string]any{"action": "second", "target": "one"}}}, allowed)
	if err != nil || config.Workflow[0].TargetURL != "https://example.com/default" || config.Workflow[1].TargetURL != "https://example.com/one" {
		t.Fatalf("step fallback: %#v err=%v", config.Workflow, err)
	}
	config, err = parseRun(map[string]any{"targets": map[string]any{"one": "https://example.com/one"}, "scenarios": []any{
		map[string]any{"name": "s", "agents": json.Number("1"), "profiles": []any{map[string]any{}},
			"workflow": []any{map[string]any{"action": "a", "target": "one"}}}}}, allowed)
	if err != nil || config.Scenarios[0].Workflow[0].TargetURL != "https://example.com/one" {
		t.Fatalf("step-only scenario target: %#v err=%v", config.Scenarios, err)
	}
}

func TestParseRunThresholds(t *testing.T) {
	allowed := map[string]bool{"example.com": true}
	scenario := map[string]any{"name": "sales", "agents": json.Number("1"), "profiles": []any{map[string]any{}},
		"target": "primary", "workflow": []any{map[string]any{"action": "read"}, map[string]any{"action": "write", "target": "secondary"}}}
	base := func() map[string]any {
		return map[string]any{"targets": map[string]any{"primary": "https://example.com/one", "secondary": "https://example.com/two"},
			"scenarios": []any{scenario}}
	}
	body := base()
	body["thresholds"] = []any{
		map[string]any{"metric": "failed_rate", "max": json.Number("0.1"), "tags": map[string]any{"scenario": "sales", "action": "read", "target": "primary"}},
		map[string]any{"metric": "latency_p95_ms", "max": json.Number("250")},
		map[string]any{"metric": "timeouts", "max": json.Number("0")},
	}
	config, err := parseRun(body, allowed)
	if err != nil || len(config.Thresholds) != 3 || config.Thresholds[0].Metric != "failed_rate" ||
		config.Thresholds[0].Max != 0.1 || config.Thresholds[0].Scenario != "sales" ||
		config.Thresholds[0].Action != "read" || config.Thresholds[0].Target != "primary" ||
		config.Thresholds[1].Max != 250 || config.Thresholds[2].Max != 0 {
		t.Fatalf("thresholds=%#v err=%v", config.Thresholds, err)
	}
	if config.Scenarios[0].Workflow[0].TargetName != "primary" || config.Scenarios[0].Workflow[1].TargetName != "secondary" {
		t.Fatalf("target names not preserved: %#v", config.Scenarios[0].Workflow)
	}
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"wrong array type", map[string]any{}}, {"wrong entry type", []any{"bad"}},
		{"missing metric", []any{map[string]any{"max": json.Number("0")}}},
		{"unknown metric", []any{map[string]any{"metric": "requests", "max": json.Number("0")}}},
		{"missing max", []any{map[string]any{"metric": "timeouts"}}},
		{"wrong max type", []any{map[string]any{"metric": "timeouts", "max": "1"}}},
		{"negative max", []any{map[string]any{"metric": "timeouts", "max": json.Number("-1")}}},
		{"infinite max", []any{map[string]any{"metric": "timeouts", "max": json.Number("1e999")}}},
		{"failed rate above one", []any{map[string]any{"metric": "failed_rate", "max": json.Number("1.1")}}},
		{"unknown field", []any{map[string]any{"metric": "timeouts", "max": json.Number("0"), "x": true}}},
		{"wrong tags type", []any{map[string]any{"metric": "timeouts", "max": json.Number("0"), "tags": []any{}}}},
		{"unknown tag", []any{map[string]any{"metric": "timeouts", "max": json.Number("0"), "tags": map[string]any{"team": "sales"}}}},
		{"wrong tag value", []any{map[string]any{"metric": "timeouts", "max": json.Number("0"), "tags": map[string]any{"action": 1}}}},
		{"unknown scenario", []any{map[string]any{"metric": "timeouts", "max": json.Number("0"), "tags": map[string]any{"scenario": "missing"}}}},
		{"unknown action", []any{map[string]any{"metric": "timeouts", "max": json.Number("0"), "tags": map[string]any{"action": "missing"}}}},
		{"unknown target", []any{map[string]any{"metric": "timeouts", "max": json.Number("0"), "tags": map[string]any{"target": "missing"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := base()
			body["thresholds"] = tc.value
			if _, err := parseRun(body, allowed); err == nil {
				t.Fatal("accepted invalid threshold")
			}
		})
	}
	config, err = parseRun(map[string]any{"target_url": "https://example.com", "thresholds": []any{
		map[string]any{"metric": "timeouts", "max": json.Number("1"), "tags": map[string]any{"action": "request"}}}}, allowed)
	if err != nil || config.Workflow[0].TargetName != "" || len(config.Thresholds) != 1 {
		t.Fatalf("default workflow threshold=%#v err=%v", config, err)
	}
}
