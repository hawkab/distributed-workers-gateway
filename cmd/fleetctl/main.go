package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fatal("usage: fleetctl init-worker [LEGACY-ID] | enqueue JSON-FILE | status [JOB-ID] | workers | metrics | cancel JOB-ID")
	}
	base := os.Getenv("WORKER_API_URL")
	if base == "" {
		base = "http://127.0.0.1:18220"
	}
	token := os.Getenv("WORKER_ADMIN_TOKEN")
	if token == "" {
		fatal("WORKER_ADMIN_TOKEN is required")
	}
	method, path := "GET", ""
	var body []byte
	switch os.Args[1] {
	case "init-worker":
		if len(os.Args) == 2 {
			method, path = "POST", "/v1/admin/enrollments"
			body = []byte(`{}`)
		} else if len(os.Args) == 3 {
			method, path = "POST", "/v1/admin/workers"
			body, _ = json.Marshal(map[string]string{"worker_id": os.Args[2]})
		} else {
			fatal("init-worker accepts one optional legacy ID")
		}
	case "enqueue":
		if len(os.Args) != 3 {
			fatal("JSON file required (or - for stdin)")
		}
		method, path = "POST", "/v1/admin/jobs"
		var e error
		if os.Args[2] == "-" {
			body, e = io.ReadAll(os.Stdin)
		} else {
			body, e = os.ReadFile(os.Args[2])
		}
		if e != nil {
			fatal("cannot read job JSON")
		}
	case "status":
		path = "/v1/admin/jobs"
		if len(os.Args) == 3 {
			path += "/" + os.Args[2]
		}
	case "workers":
		path = "/v1/admin/workers"
	case "metrics":
		path = "/v1/admin/metrics"
	case "cancel":
		if len(os.Args) != 3 {
			fatal("job ID required")
		}
		method, path = "POST", "/v1/admin/jobs/"+os.Args[2]+"/cancel"
	default:
		fatal("unknown command")
	}
	req, e := http.NewRequest(method, strings.TrimRight(base, "/")+path, bytes.NewReader(body))
	if e != nil {
		fatal("invalid API URL")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		fatal("gateway request failed")
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "gateway HTTP %d\n", res.StatusCode)
		os.Exit(1)
	}
	io.Copy(os.Stdout, res.Body)
}
func fatal(s string) { fmt.Fprintln(os.Stderr, s); os.Exit(1) }
