package stress

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type step struct {
	Action       string
	Input        map[string]any
	ExpectStatus []int
}

type runConfig struct {
	TargetURL, TelemetryURL          string
	Profiles                         []map[string]any
	Workflow                         []step
	Agents, Concurrency, ArrivalRate int
	ThinkMS, JitterMS, TimeoutMS     int
	Retries, Seed, Workers           int
}

var actionPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

func readJSON(r *http.Request) (map[string]any, error) {
	if !strings.EqualFold(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]), "application/json") {
		return nil, errors.New("content-type must be application/json")
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 65537))
	if err != nil {
		return nil, errors.New("request body must be a JSON object")
	}
	if len(data) > 65536 {
		return nil, errors.New("request body exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var body map[string]any
	if decoder.Decode(&body) != nil || body == nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("request body must be a JSON object")
	}
	return body, nil
}

func number(body map[string]any, key string, fallback, min int) (int, error) {
	value := body[key]
	if value == nil {
		return fallback, nil
	}
	text, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("%s must be an integer at least %d", key, min)
	}
	n, err := strconv.Atoi(text.String())
	if err != nil || n < min {
		return 0, fmt.Errorf("%s must be an integer at least %d", key, min)
	}
	return n, nil
}

func parseProfiles(value any) ([]map[string]any, error) {
	items, ok := value.([]any)
	if !ok || len(items) < 1 {
		return nil, errors.New("profiles must contain at least one JSON object")
	}
	profiles := make([]map[string]any, len(items))
	for index, item := range items {
		profile, ok := item.(map[string]any)
		if !ok {
			return nil, errors.New("profiles must contain at least one JSON object")
		}
		profiles[index] = profile
	}
	return profiles, nil
}

func parseWorkflow(value any) ([]step, error) {
	items, ok := value.([]any)
	if !ok || len(items) < 1 {
		return nil, errors.New("workflow must contain at least one step")
	}
	workflow := make([]step, len(items))
	invalid := errors.New("workflow steps need an action, optional input object, and optional expect_status array")
	for index, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, invalid
		}
		action, ok := object["action"].(string)
		if !ok || !actionPattern.MatchString(action) {
			return nil, invalid
		}
		input := map[string]any{}
		if value, present := object["input"]; present {
			input, ok = value.(map[string]any)
			if !ok {
				return nil, invalid
			}
		}
		var expected []int
		if value := object["expect_status"]; value != nil {
			statuses, ok := value.([]any)
			if !ok || len(statuses) < 1 {
				return nil, invalid
			}
			for _, status := range statuses {
				n, ok := status.(json.Number)
				if !ok {
					return nil, invalid
				}
				value, err := n.Float64()
				if err != nil || math.Trunc(value) != value || value < 100 || value > 599 {
					return nil, invalid
				}
				expected = append(expected, int(value))
			}
		}
		workflow[index] = step{action, input, expected}
	}
	return workflow, nil
}

func targetURL(value any, allowed map[string]bool) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", errors.New("target_url must be a URL")
	}
	u, err := url.Parse(text)
	if err != nil {
		return "", errors.New("target_url must be a URL")
	}
	if (u.Scheme != "http" && u.Scheme != "https") || !allowed[strings.ToLower(u.Hostname())] ||
		u.User != nil || u.Fragment != "" || u.Host == "" {
		return "", errors.New("target_url must be HTTP(S) on an allowed host without credentials")
	}
	return u.String(), nil
}

func parseRun(body map[string]any, allowed map[string]bool) (runConfig, error) {
	var config runConfig
	var err error
	if config.TargetURL, err = targetURL(body["target_url"], allowed); err != nil {
		return config, err
	}
	if value, ok := body["telemetry_url"]; ok {
		if config.TelemetryURL, err = targetURL(value, allowed); err != nil {
			return config, err
		}
	}
	profiles := body["profiles"]
	if profiles == nil {
		profiles = []any{map[string]any{}}
	}
	if config.Profiles, err = parseProfiles(profiles); err != nil {
		return config, err
	}
	workflow := body["workflow"]
	if workflow == nil {
		workflow = []any{map[string]any{"action": "request"}}
	}
	if config.Workflow, err = parseWorkflow(workflow); err != nil {
		return config, err
	}
	if config.Agents, err = number(body, "agents", 1, 1); err != nil {
		return config, err
	}
	if config.Concurrency, err = number(body, "concurrency", 10, 1); err != nil {
		return config, err
	}
	if config.ArrivalRate, err = number(body, "arrival_rate", 10, 1); err != nil {
		return config, err
	}
	if config.ThinkMS, err = number(body, "think_ms", 50, 0); err != nil {
		return config, err
	}
	if config.JitterMS, err = number(body, "jitter_ms", 50, 0); err != nil {
		return config, err
	}
	if config.TimeoutMS, err = number(body, "timeout_ms", 5000, 1); err != nil {
		return config, err
	}
	if config.Retries, err = number(body, "retries", 1, 0); err != nil {
		return config, err
	}
	if config.Seed, err = number(body, "seed", 1, 0); err != nil {
		return config, err
	}
	workers := runtime.NumCPU()
	if workers > 4 {
		workers = 4
	}
	if config.Agents < 1000 {
		workers = 1
	}
	if config.Workers, err = number(body, "workers", workers, 1); err != nil {
		return config, err
	}
	maxMS := int(math.MaxInt64 / int64(time.Millisecond))
	if config.TimeoutMS > maxMS {
		return config, errors.New("timeout_ms exceeds the supported duration range")
	}
	return config, nil
}
