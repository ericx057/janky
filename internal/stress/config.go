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
	TargetURL    string
	TargetName   string
}

type thresholdConfig struct {
	Metric                   string
	Max                      float64
	Scenario, Action, Target string
}

type scenarioConfig struct {
	Name                     string
	Agents                   int
	ArrivalRate, Concurrency int
	Profiles                 []map[string]any
	Workflow                 []step
	TargetURL                string
}

type runConfig struct {
	TargetURL, TelemetryURL          string
	Name                             string
	Profiles                         []map[string]any
	Workflow                         []step
	Scenarios                        []scenarioConfig
	Thresholds                       []thresholdConfig
	Agents, Concurrency, ArrivalRate int
	ThinkMS, JitterMS, TimeoutMS     int
	Retries, Seed, Workers           int
}

var actionPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

func parseScenarios(value any) ([]scenarioConfig, int, error) {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil, 0, errors.New("scenarios must contain at least one object")
	}
	scenarios := make([]scenarioConfig, 0, len(items))
	seen := make(map[string]bool, len(items))
	total := 0
	for _, value := range items {
		item, ok := value.(map[string]any)
		if !ok {
			return nil, 0, errors.New("scenario must be an object")
		}
		name, ok := item["name"].(string)
		if !ok || !actionPattern.MatchString(name) || seen[name] {
			return nil, 0, errors.New("scenario names must be unique and contain only letters, numbers, _, ., or -")
		}
		seen[name] = true
		if item["agents"] == nil {
			return nil, 0, errors.New("scenario agents is required")
		}
		agents, err := number(item, "agents", 0, 1)
		if err != nil {
			return nil, 0, err
		}
		arrivalRate, err := number(item, "arrival_rate", 0, 1)
		if err != nil {
			return nil, 0, err
		}
		concurrency, err := number(item, "concurrency", 0, 1)
		if err != nil {
			return nil, 0, err
		}
		profiles, err := parseProfiles(item["profiles"])
		if err != nil {
			return nil, 0, err
		}
		workflow, err := parseWorkflow(item["workflow"])
		if err != nil {
			return nil, 0, err
		}
		target := ""
		if value, present := item["target"]; present {
			target, ok = value.(string)
			if !ok || !actionPattern.MatchString(target) {
				return nil, 0, errors.New("scenario target must be a valid target name")
			}
		}
		if agents > math.MaxInt-total {
			return nil, 0, errors.New("total scenario agents exceeds the supported range")
		}
		total += agents
		scenarios = append(scenarios, scenarioConfig{Name: name, Agents: agents, ArrivalRate: arrivalRate, Concurrency: concurrency,
			Profiles: profiles, Workflow: workflow, TargetURL: target})
	}
	return scenarios, total, nil
}

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
	invalid := errors.New("workflow steps need an action, optional input object, optional expect_status array, and optional target name")
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
		target := ""
		if value, present := object["target"]; present {
			target, ok = value.(string)
			if !ok || !actionPattern.MatchString(target) {
				return nil, invalid
			}
		}
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
		workflow[index] = step{Action: action, Input: input, ExpectStatus: expected, TargetURL: target}
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

func resolveTargets(workflow []step, fallback, fallbackName string, targets map[string]string) error {
	for index := range workflow {
		if workflow[index].TargetURL != "" {
			name := workflow[index].TargetURL
			url := targets[name]
			if url == "" {
				return fmt.Errorf("unknown target %q", name)
			}
			workflow[index].TargetURL = url
			workflow[index].TargetName = name
		} else if fallback == "" {
			return errors.New("each workflow step needs a target or target_url")
		} else {
			workflow[index].TargetURL = fallback
			workflow[index].TargetName = fallbackName
		}
	}
	return nil
}

func parseThresholds(value any, config runConfig, targets map[string]string) ([]thresholdConfig, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("thresholds must be an array")
	}
	validScenarios := map[string]bool{}
	validActions := map[string]bool{}
	for _, scenario := range config.Scenarios {
		validScenarios[scenario.Name] = true
		for _, item := range scenario.Workflow {
			validActions[item.Action] = true
		}
	}
	for _, item := range config.Workflow {
		validActions[item.Action] = true
	}
	thresholds := make([]thresholdConfig, 0, len(items))
	for _, value := range items {
		item, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("threshold must be an object")
		}
		for key := range item {
			if key != "metric" && key != "max" && key != "tags" {
				return nil, fmt.Errorf("unknown threshold field %q", key)
			}
		}
		metric, ok := item["metric"].(string)
		if !ok || (metric != "failed_rate" && metric != "latency_p95_ms" && metric != "timeouts") {
			return nil, errors.New("threshold metric must be failed_rate, latency_p95_ms, or timeouts")
		}
		number, ok := item["max"].(json.Number)
		if !ok {
			return nil, errors.New("threshold max must be a finite nonnegative number")
		}
		max, err := number.Float64()
		if err != nil || math.IsNaN(max) || math.IsInf(max, 0) || max < 0 || (metric == "failed_rate" && max > 1) {
			return nil, errors.New("threshold max must be finite, nonnegative, and at most 1 for failed_rate")
		}
		threshold := thresholdConfig{Metric: metric, Max: max}
		if value, present := item["tags"]; present {
			tags, ok := value.(map[string]any)
			if !ok {
				return nil, errors.New("threshold tags must be an object")
			}
			for key, value := range tags {
				name, ok := value.(string)
				if !ok || name == "" {
					return nil, fmt.Errorf("threshold tag %q must be a configured name", key)
				}
				switch key {
				case "scenario":
					if !validScenarios[name] {
						return nil, fmt.Errorf("unknown scenario %q", name)
					}
					threshold.Scenario = name
				case "action":
					if !validActions[name] {
						return nil, fmt.Errorf("unknown action %q", name)
					}
					threshold.Action = name
				case "target":
					if targets[name] == "" {
						return nil, fmt.Errorf("unknown target %q", name)
					}
					threshold.Target = name
				default:
					return nil, fmt.Errorf("unknown threshold tag %q", key)
				}
			}
		}
		thresholds = append(thresholds, threshold)
	}
	return thresholds, nil
}

func parseRun(body map[string]any, allowed map[string]bool) (runConfig, error) {
	var config runConfig
	var err error
	if value, present := body["target_url"]; present {
		if config.TargetURL, err = targetURL(value, allowed); err != nil {
			return config, err
		}
	}
	targets := map[string]string{}
	if value, present := body["targets"]; present {
		items, ok := value.(map[string]any)
		if !ok || len(items) == 0 {
			return config, errors.New("targets must be a nonempty object")
		}
		for name, value := range items {
			if !actionPattern.MatchString(name) {
				return config, errors.New("target names must contain only letters, numbers, _, ., or -")
			}
			url, err := targetURL(value, allowed)
			if err != nil {
				return config, err
			}
			targets[name] = url
		}
	}
	if value, ok := body["telemetry_url"]; ok {
		if config.TelemetryURL, err = targetURL(value, allowed); err != nil {
			return config, err
		}
	}
	if scenarios, present := body["scenarios"]; present {
		for _, key := range []string{"agents", "profiles", "workflow"} {
			if _, mixed := body[key]; mixed {
				return config, errors.New("scenarios cannot be combined with top-level agents, profiles, or workflow")
			}
		}
		config.Scenarios, config.Agents, err = parseScenarios(scenarios)
		if err != nil {
			return config, err
		}
		for index := range config.Scenarios {
			scenario := &config.Scenarios[index]
			fallback := config.TargetURL
			fallbackName := scenario.TargetURL
			if fallbackName != "" {
				fallback = targets[fallbackName]
				if fallback == "" {
					return config, fmt.Errorf("unknown target %q", fallbackName)
				}
			}
			scenario.TargetURL = fallback
			if err := resolveTargets(scenario.Workflow, fallback, fallbackName, targets); err != nil {
				return config, err
			}
		}
	} else {
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
		if err := resolveTargets(config.Workflow, config.TargetURL, "", targets); err != nil {
			return config, err
		}
		if config.Agents, err = number(body, "agents", 1, 1); err != nil {
			return config, err
		}
	}
	if value, present := body["thresholds"]; present {
		if config.Thresholds, err = parseThresholds(value, config, targets); err != nil {
			return config, err
		}
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
