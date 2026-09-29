package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type randomSource uint32

func (source *randomSource) next() float64 {
	*source = 1664525**source + 1013904223
	return float64(*source) / 4294967296
}

func eventFor(id string, profile map[string]any, index int, workflowID string, workflow []step) map[string]any {
	return map[string]any{
		"workflow_id": workflowID, "workflow_step": index + 1,
		"action": workflow[index].Action,
		"agent":  map[string]any{"id": id, "profile": profile}, "input": workflow[index].Input,
	}
}

func expectedStatus(step step, status int) bool {
	if step.ExpectStatus == nil {
		return status >= 200 && status < 300
	}
	for _, accepted := range step.ExpectStatus {
		if accepted == status {
			return true
		}
	}
	return false
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if response != nil {
		header := response.Header.Get("Retry-After")
		if seconds, err := strconv.ParseFloat(header, 64); err == nil && !math.IsNaN(seconds) && seconds >= 0 {
			return time.Duration(math.Min(1000, seconds*1000)) * time.Millisecond
		}
		if date, err := http.ParseTime(header); err == nil {
			wait := time.Until(date)
			if wait < 0 {
				return 0
			}
			if wait > time.Second {
				return time.Second
			}
			return wait
		}
	}
	return time.Duration(50*(1<<attempt)) * time.Millisecond
}

func (s *server) runEvent(run *runState, event map[string]any) {
	if run != nil {
		event["run_id"] = run.ID
	}
	s.emit(event)
}

func (s *server) dispatch(target string, event map[string]any, step step, timeoutMS, retries int, run *runState) bool {
	body, err := json.Marshal(event)
	if err != nil {
		return false
	}
	agent := event["agent"].(map[string]any)["id"].(string)
	for attempt := 0; ; attempt++ {
		started := time.Now()
		s.mu.Lock()
		s.global.InFlight++
		if run != nil {
			run.Metrics.InFlight++
		}
		s.mu.Unlock()
		s.runEvent(run, map[string]any{"type": "request_started", "agent_id": agent, "action": step.Action})
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMS)*time.Millisecond)
		request, requestError := http.NewRequestWithContext(ctx, "POST", target, bytes.NewReader(body))
		var response *http.Response
		if requestError == nil {
			request.Header.Set("Content-Type", "application/json")
			response, requestError = s.client.Do(request)
			if requestError == nil {
				_, requestError = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
		}
		cancel()
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		expected := requestError == nil && expectedStatus(step, status)
		timedOut := errors.Is(requestError, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)
		errorName := any(nil)
		if requestError != nil {
			errorName = "TypeError"
			if timedOut {
				errorName = "TimeoutError"
			}
		}
		latency := float64(time.Since(started)) / float64(time.Millisecond)
		s.mu.Lock()
		s.global.InFlight--
		s.global.record(latency, status, timedOut, expected)
		if run != nil {
			run.Metrics.InFlight--
			run.Metrics.record(latency, status, timedOut, expected)
		}
		s.mu.Unlock()
		s.runEvent(run, map[string]any{"type": "request", "workflow_id": event["workflow_id"],
			"agent_id": agent, "action": step.Action, "status": status,
			"error": errorName, "expected": expected, "latency_ms": latency})
		if expected {
			return true
		}
		if attempt >= retries || (response != nil && requestError == nil && status != 429 && status < 500) {
			return false
		}
		s.mu.Lock()
		s.global.Retries++
		if run != nil {
			run.Metrics.Retries++
		}
		s.mu.Unlock()
		s.runEvent(run, map[string]any{"type": "retry", "agent_id": agent})
		time.Sleep(retryDelay(response, attempt))
	}
}

func (s *server) sampleTelemetry(run *runState, url string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	var response *http.Response
	if err == nil {
		response, err = s.client.Do(request)
	}
	var value any
	if err == nil {
		var body []byte
		body, err = io.ReadAll(io.LimitReader(response.Body, 16385))
		_ = response.Body.Close()
		if err == nil && len(body) > 16384 {
			err = errors.New("telemetry response exceeds 16 KiB")
		}
		if err == nil {
			value = string(body)
			if strings.Contains(response.Header.Get("Content-Type"), "json") {
				var parsed any
				if decodeError := json.Unmarshal(body, &parsed); decodeError != nil {
					err = decodeError
				} else {
					value = parsed
				}
			}
		}
	}
	telemetry := map[string]any{"at": time.Now().UTC().Format(time.RFC3339Nano)}
	if err == nil {
		telemetry["status"] = response.StatusCode
		telemetry["data"] = value
	} else {
		message := "telemetry request failed"
		if errors.Is(err, context.DeadlineExceeded) {
			message = "telemetry request timed out"
		}
		if err.Error() == "telemetry response exceeds 16 KiB" {
			message = err.Error()
		}
		var syntax *json.SyntaxError
		if errors.As(err, &syntax) {
			message = "invalid telemetry JSON"
		}
		telemetry["error"] = message
	}
	s.mu.Lock()
	run.Telemetry = telemetry
	s.mu.Unlock()
}

func (s *server) runAgent(run *runState, config runConfig, index int) {
	id := fmt.Sprintf("%s-%d", run.ID, index+1)
	profile := config.Profiles[index%len(config.Profiles)]
	random := randomSource(uint32(config.Seed + index + 1))
	s.mu.Lock()
	run.Agents.Started++
	s.totals.Started++
	s.mu.Unlock()
	s.runEvent(run, map[string]any{"type": "agent_started", "agent_id": id})
	workflowID := uuid()
	succeeded := true
	for position, item := range config.Workflow {
		if position > 0 {
			pause := float64(config.ThinkMS)*(0.5+random.next()) + random.next()*float64(config.JitterMS)
			time.Sleep(time.Duration(pause * float64(time.Millisecond)))
		}
		event := eventFor(id, profile, position, workflowID, config.Workflow)
		if !s.dispatch(config.TargetURL, event, item, config.TimeoutMS, config.Retries, run) {
			succeeded = false
			break
		}
	}
	s.mu.Lock()
	if succeeded {
		run.Agents.Completed++
		s.totals.Completed++
	} else {
		run.Agents.Failed++
		s.totals.Failed++
	}
	s.mu.Unlock()
	s.runEvent(run, map[string]any{"type": "agent_finished", "agent_id": id, "succeeded": succeeded})
}

func (s *server) runFleet(run *runState, config runConfig) {
	stopTelemetry := make(chan struct{})
	var telemetryDone sync.WaitGroup
	if config.TelemetryURL != "" {
		telemetryDone.Add(1)
		go func() {
			defer telemetryDone.Done()
			s.sampleTelemetry(run, config.TelemetryURL)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopTelemetry:
					return
				case <-ticker.C:
					s.sampleTelemetry(run, config.TelemetryURL)
				}
			}
		}()
	}
	start := time.Now()
	limit := make(chan struct{}, config.Concurrency)
	var agents sync.WaitGroup
	for index := 0; index < config.Agents; index++ {
		jitter := randomSource(uint32(config.Seed + index))
		due := float64(index)*1000/float64(config.ArrivalRate) + jitter.next()*float64(config.JitterMS)
		if wait := time.Until(start.Add(time.Duration(due * float64(time.Millisecond)))); wait > 0 {
			time.Sleep(wait)
		}
		limit <- struct{}{}
		agents.Add(1)
		go func(index int) {
			defer agents.Done()
			defer func() { <-limit }()
			s.runAgent(run, config, index)
		}(index)
	}
	agents.Wait()
	close(stopTelemetry)
	telemetryDone.Wait()
	s.mu.Lock()
	run.State = "completed"
	run.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	s.activeRun = nil
	s.mu.Unlock()
	s.runEvent(run, map[string]any{"type": "run_finished", "state": "completed"})
}
