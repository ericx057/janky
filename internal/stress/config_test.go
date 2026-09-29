package stress

import (
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
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
		{"above", json.Number("11"), 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := number(map[string]any{"value": tc.value}, "value", 7, 1, 10)
			if n != tc.want || (err != nil) != tc.invalid {
				t.Fatalf("number=%d err=%v", n, err)
			}
		})
	}
}

func TestParseProfiles(t *testing.T) {
	for _, value := range []any{nil, []any{}, make([]any, 101), []any{"bad"}} {
		if _, err := parseProfiles(value); err == nil {
			t.Errorf("accepted profiles %#v", value)
		}
	}
	profiles, err := parseProfiles([]any{map[string]any{"id": "a"}})
	if err != nil || len(profiles) != 1 || profiles[0]["id"] != "a" {
		t.Fatalf("profiles=%#v err=%v", profiles, err)
	}
}

func TestParseWorkflow(t *testing.T) {
	for _, value := range []any{
		nil, []any{}, make([]any, 33), []any{"bad"},
		[]any{map[string]any{}},
		[]any{map[string]any{"action": "bad space"}},
		[]any{map[string]any{"action": "ok", "input": []any{}}},
		[]any{map[string]any{"action": "ok", "expect_status": "200"}},
		[]any{map[string]any{"action": "ok", "expect_status": []any{}}},
		[]any{map[string]any{"action": "ok", "expect_status": make([]any, 11)}},
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
		{"retries", json.Number("6")}, {"seed", json.Number("-1")},
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
}
