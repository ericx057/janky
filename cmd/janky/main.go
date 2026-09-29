package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ericx057/janky/internal/stress"
)

var exitProcess = os.Exit
var listenAndServe = func(address string, handler http.Handler) error {
	return (&http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe()
}

func runCLI(args []string, lookupEnv func(string) (string, bool), serve func(string, http.Handler) error) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: janky <port>")
	}
	port, err := strconv.Atoi(args[0])
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("port must be an integer from 1 to 65535")
	}
	var hosts []string
	if value, ok := lookupEnv("ALLOWED_TARGET_HOSTS"); ok {
		hosts = strings.Split(value, ",")
	}
	address := fmt.Sprintf("127.0.0.1:%d", port)
	log.Printf("agent stress tester listening on http://%s", address)
	return serve(address, stress.NewServer(hosts))
}

func main() {
	if err := runCLI(os.Args[1:], os.LookupEnv, listenAndServe); err != nil {
		log.Print(err)
		exitProcess(1)
	}
}
