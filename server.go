package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

type agentState struct {
	Profile    map[string]any
	Workflow   []step
	Step       int
	WorkflowID string
	Retry      bool
}

type runState struct {
	ID, State, StartedAt string
	FinishedAt, Error    any
	Agents               struct{ Started, Completed, Failed int }
	Metrics              metrics
	Telemetry            any
}

type server struct {
	mu             sync.Mutex
	allowed        map[string]bool
	client         *http.Client
	runs           map[string]*runState
	runOrder       []string
	agents         map[string]agentState
	agentOrder     []string
	busy           map[string]bool
	activeRun      *runState
	directInFlight int
	listeners      map[chan []byte]bool
	global         metrics
	totals         struct{ Started, Completed, Failed int }
}

var agentPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
var runPattern = regexp.MustCompile(`^[a-f0-9-]+$`)

func NewServer(allowedHosts []string) http.Handler {
	if allowedHosts == nil {
		allowedHosts = []string{"127.0.0.1", "localhost", "::1"}
	}
	allowed := make(map[string]bool, len(allowedHosts))
	for _, host := range allowedHosts {
		allowed[strings.ToLower(strings.Trim(strings.TrimSpace(host), "[]"))] = true
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &server{
		allowed: allowed,
		client:  &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		runs:    make(map[string]*runState), agents: make(map[string]agentState),
		busy: make(map[string]bool), listeners: make(map[chan []byte]bool),
	}
}

func uuid() string {
	var value [16]byte
	_, _ = rand.Read(value[:])
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:])
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (s *server) emit(event map[string]any) {
	line, err := json.Marshal(event)
	if err != nil {
		return
	}
	line = append(append([]byte("data: "), line...), '\n', '\n')
	s.mu.Lock()
	for listener := range s.listeners {
		select {
		case listener <- line:
		default:
			delete(s.listeners, listener)
			close(listener)
		}
	}
	s.mu.Unlock()
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case r.Method == "GET" && path == "/health":
		writeJSON(w, 200, map[string]bool{"ok": true})
	case r.Method == "GET" && path == "/events":
		s.events(w, r)
	case r.Method == "GET" && path == "/status":
		s.status(w)
	case r.Method == "GET" && path == "/metrics":
		s.prometheus(w)
	case r.Method == "GET" && strings.HasPrefix(path, "/runs/") && runPattern.MatchString(strings.TrimPrefix(path, "/runs/")):
		s.getRun(w, strings.TrimPrefix(path, "/runs/"))
	case r.Method == "POST" && path == "/runs":
		s.startRun(w, r)
	case r.Method == "POST" && strings.HasPrefix(path, "/agents/") && strings.HasSuffix(path, "/invoke") &&
		agentPattern.MatchString(strings.TrimSuffix(strings.TrimPrefix(path, "/agents/"), "/invoke")):
		s.invoke(w, r, strings.TrimSuffix(strings.TrimPrefix(path, "/agents/"), "/invoke"))
	default:
		writeError(w, 404, "not found")
	}
}

func (s *server) events(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if len(s.listeners) >= 32 {
		s.mu.Unlock()
		writeError(w, 503, "too many event observers")
		return
	}
	listener := make(chan []byte, 256)
	s.listeners[listener] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.listeners[listener] {
			delete(s.listeners, listener)
			close(listener)
		}
		s.mu.Unlock()
	}()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	_, _ = w.Write([]byte(": connected\n\n"))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case line, ok := <-listener:
			if !ok {
				return
			}
			if _, err := w.Write(line); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}
}

func (s *server) runSnapshot(run *runState) map[string]any {
	errorValue := run.Error
	return map[string]any{
		"run_id": run.ID, "state": run.State,
		"agents":   map[string]int{"started": run.Agents.Started, "completed": run.Agents.Completed, "failed": run.Agents.Failed},
		"requests": run.Metrics.snapshot(), "started_at": run.StartedAt,
		"finished_at": run.FinishedAt, "target_telemetry": run.Telemetry, "error": errorValue,
	}
}

func (s *server) getRun(w http.ResponseWriter, id string) {
	s.mu.Lock()
	run := s.runs[id]
	var result map[string]any
	if run != nil {
		result = s.runSnapshot(run)
	}
	s.mu.Unlock()
	if run == nil {
		writeError(w, 404, "run not found")
	} else {
		writeJSON(w, 200, result)
	}
}

func (s *server) status(w http.ResponseWriter) {
	s.mu.Lock()
	runs := make([]map[string]any, 0, len(s.runOrder))
	for _, id := range s.runOrder {
		run := s.runs[id]
		runs = append(runs, map[string]any{"run_id": id, "state": run.State, "target_telemetry": run.Telemetry})
	}
	result := map[string]any{
		"agents":   map[string]int{"started": s.totals.Started, "completed": s.totals.Completed, "failed": s.totals.Failed},
		"requests": s.global.snapshot(), "runs": runs,
	}
	s.mu.Unlock()
	writeJSON(w, 200, result)
}

func (s *server) prometheus(w http.ResponseWriter) {
	s.mu.Lock()
	metric := s.global.snapshot()
	totals := s.totals
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	for _, item := range []struct {
		name  string
		value int
	}{
		{"agent_stress_agents_started_total", totals.Started},
		{"agent_stress_agents_completed_total", totals.Completed},
		{"agent_stress_agents_failed_total", totals.Failed},
	} {
		_, _ = fmt.Fprintf(w, "%s %d\n", item.name, item.value)
	}
	for _, key := range []string{"total", "succeeded", "failed", "throttled", "server_errors", "expected_non_2xx",
		"expectation_failures", "timeouts", "retries", "in_flight", "latency_avg_ms", "latency_p95_ms"} {
		_, _ = fmt.Fprintf(w, "agent_stress_requests_%s %v\n", key, metric[key])
	}
}

func (s *server) startRun(w http.ResponseWriter, r *http.Request) {
	body, err := readJSON(r)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	config, err := parseRun(body, s.allowed)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	s.mu.Lock()
	if s.directInFlight > 0 || s.activeRun != nil {
		s.mu.Unlock()
		writeError(w, 409, "another workload is active")
		return
	}
	run := &runState{ID: uuid(), State: "running", StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	s.runs[run.ID] = run
	s.runOrder = append(s.runOrder, run.ID)
	if len(s.runOrder) > 100 {
		delete(s.runs, s.runOrder[0])
		s.runOrder = s.runOrder[1:]
	}
	s.activeRun = run
	s.mu.Unlock()
	go s.runFleet(run, config)
	writeJSON(w, 202, map[string]string{"run_id": run.ID, "status_url": "/runs/" + run.ID})
}

func (s *server) saveAgent(id string, state agentState) {
	if _, exists := s.agents[id]; !exists {
		s.agentOrder = append(s.agentOrder, id)
	}
	s.agents[id] = state
	if len(s.agentOrder) > 10000 {
		delete(s.agents, s.agentOrder[0])
		s.agentOrder = s.agentOrder[1:]
	}
}

func (s *server) invoke(w http.ResponseWriter, r *http.Request, id string) {
	body, err := readJSON(r)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	s.mu.Lock()
	previous, exists := s.agents[id]
	profile := previous.Profile
	if !exists {
		profile = map[string]any{}
	}
	if value := body["profile"]; value != nil {
		parsed, ok := value.(map[string]any)
		if !ok {
			s.mu.Unlock()
			writeError(w, 400, "profiles must contain 1 to 100 JSON objects")
			return
		}
		profile = parsed
	}
	workflow := previous.Workflow
	if !exists {
		workflow = []step{{Action: "request", Input: map[string]any{}}}
	}
	if value := body["workflow"]; value != nil {
		workflow, err = parseWorkflow(value)
		if err != nil {
			s.mu.Unlock()
			writeError(w, 400, err.Error())
			return
		}
	}
	url := ""
	if value, present := body["target_url"]; present {
		url, err = targetURL(value, s.allowed)
		if err != nil {
			s.mu.Unlock()
			writeError(w, 400, err.Error())
			return
		}
	}
	timeout, err := number(body, "timeout_ms", 5000, 1, 120000)
	if err != nil {
		s.mu.Unlock()
		writeError(w, 400, err.Error())
		return
	}
	retries, err := number(body, "retries", 1, 0, 5)
	if err != nil {
		s.mu.Unlock()
		writeError(w, 400, err.Error())
		return
	}
	if s.busy[id] || s.activeRun != nil {
		s.mu.Unlock()
		writeError(w, 409, "agent or fleet is busy")
		return
	}
	if url != "" && s.directInFlight >= 256 {
		s.mu.Unlock()
		writeError(w, 429, "too many direct invocations")
		return
	}
	index := previous.Step % len(workflow)
	workflowID := previous.WorkflowID
	if index == 0 && !previous.Retry {
		workflowID = uuid()
	}
	event := eventFor(id, profile, index, workflowID, workflow)
	arguments, err := json.Marshal(event)
	if err != nil {
		s.mu.Unlock()
		writeError(w, 500, "internal error")
		return
	}
	next := agentState{Profile: profile, Workflow: workflow, Step: (index + 1) % len(workflow), WorkflowID: workflowID}
	if url != "" {
		s.busy[id] = true
		s.directInFlight++
	} else {
		s.saveAgent(id, next)
	}
	s.mu.Unlock()
	var delivered any
	if url != "" {
		result := s.dispatch(url, event, workflow[index], timeout, retries, nil)
		delivered = result
		s.mu.Lock()
		delete(s.busy, id)
		s.directInFlight--
		if result {
			s.saveAgent(id, next)
		} else {
			s.saveAgent(id, agentState{Profile: profile, Workflow: workflow, Step: index, WorkflowID: workflowID, Retry: true})
		}
		s.mu.Unlock()
	}
	writeJSON(w, 200, map[string]any{
		"agent_id": id, "workflow_id": workflowID, "step": index + 1, "delivered": delivered,
		"message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{
			"id": uuid(), "type": "function", "function": map[string]any{"name": "workflow_request", "arguments": string(arguments)},
		}}},
	})
}
