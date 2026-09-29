package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestRunCLIValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"missing port", nil, "usage: janky <port>"},
		{"extra argument", []string{"8080", "extra"}, "usage: janky <port>"},
		{"noninteger", []string{"abc"}, "port must be an integer"},
		{"zero", []string{"0"}, "port must be an integer"},
		{"too large", []string{"65536"}, "port must be an integer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			err := runCLI(tc.args, func(string) (string, bool) {
				t.Fatal("looked up hosts for an invalid port")
				return "", false
			}, func(string, http.Handler) error {
				called = true
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) || called {
				t.Fatalf("error=%v, served=%v", err, called)
			}
		})
	}
}

func TestRunCLIHostsAndServeError(t *testing.T) {
	serveError := errors.New("server stopped")
	for _, tc := range []struct {
		name, hosts string
		present     bool
	}{
		{"default hosts", "", false},
		{"explicit hosts", "example.com,LOCALHOST", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			err := runCLI([]string{"65535"}, func(key string) (string, bool) {
				if key != "ALLOWED_TARGET_HOSTS" {
					t.Fatalf("unexpected env key %q", key)
				}
				return tc.hosts, tc.present
			}, func(address string, handler http.Handler) error {
				called = true
				if address != "127.0.0.1:65535" {
					t.Errorf("address=%q", address)
				}
				if handler == nil {
					t.Fatal("server handler is nil")
				}
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
				if recorder.Code != http.StatusOK {
					t.Errorf("health status=%d", recorder.Code)
				}
				return serveError
			})
			if !called || !errors.Is(err, serveError) {
				t.Fatalf("called=%v error=%v", called, err)
			}
		})
	}
}

func TestListenAndServeRejectsInvalidAddress(t *testing.T) {
	if err := listenAndServe("127.0.0.1:invalid", http.NewServeMux()); err == nil {
		t.Fatal("invalid listen address was accepted")
	}
}

func TestMainSuccessAndFailure(t *testing.T) {
	originalArgs, originalServe, originalExit := os.Args, listenAndServe, exitProcess
	t.Cleanup(func() {
		os.Args, listenAndServe, exitProcess = originalArgs, originalServe, originalExit
	})
	os.Args = []string{"janky", "8080"}
	served, exitCode := false, 0
	listenAndServe = func(address string, handler http.Handler) error {
		served = true
		if address != "127.0.0.1:8080" || handler == nil {
			t.Errorf("address=%q, handler=%v", address, handler)
		}
		return nil
	}
	exitProcess = func(code int) { exitCode = code }
	main()
	if !served || exitCode != 0 {
		t.Fatalf("success: served=%v exit=%d", served, exitCode)
	}
	listenAndServe = func(string, http.Handler) error { return errors.New("listen failed") }
	main()
	if exitCode != 1 {
		t.Fatalf("failure: exit=%d", exitCode)
	}
}
