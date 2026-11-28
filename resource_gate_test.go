package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestResourceGateIsFencedAndSharedWithoutWaitingForCompletion(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	admin := "test-only-admin-resource"
	api := &API{Store: s, AdminToken: admin}
	h := api.Handler()
	authW := request(t, h, "POST", "/v1/admin/workers", admin, `{"worker_id":"worker-a"}`)
	if authW.Code != 201 {
		t.Fatal(authW.Code)
	}
	var device map[string]string
	must(t, json.Unmarshal(authW.Body.Bytes(), &device))
	enq := request(t, h, "POST", "/v1/admin/jobs", admin, `{"operation":"text.normalize","input":{"payload":{"text":"Only this job"}},"network":{"resource_gate":{"resource":"provider-a","input_field":"payload"}}}`)
	if enq.Code != 201 {
		t.Fatal(enq.Code, enq.Body.String())
	}
	claim := request(t, h, "POST", "/v1/jobs/claim", device["token"], `{"worker_id":"worker-a","capabilities":["text.normalize"]}`)
	if claim.Code != 200 {
		t.Fatal(claim.Code)
	}
	var response struct {
		Job struct {
			ID    string `json:"id"`
			Input struct {
				Payload struct {
					Grant string `json:"resource_gate_grant"`
				} `json:"payload"`
			} `json:"input"`
		} `json:"job"`
	}
	must(t, json.Unmarshal(claim.Body.Bytes(), &response))
	grant := response.Job.Input.Payload.Grant
	if grant == "" {
		t.Fatal("missing grant")
	}
	var stored bool
	must(t, s.Pool.QueryRow(ctx, `select (input->'payload')?'resource_gate_grant' from `+s.table("jobs")+` where id=$1`, response.Job.ID).Scan(&stored))
	if stored {
		t.Fatal("authority entered stored payload")
	}
	call := func(token, action, key, ticket string, after int64) (int, gateReply) {
		t.Helper()
		path := "/v1/resource-permit"
		if token == admin {
			path = "/v1/admin/resource-permit"
		}
		b, _ := json.Marshal(gateCommand{Action: action, Resource: key, Ticket: ticket, RetryAfter: after})
		w := request(t, h, "POST", path, token, string(b))
		var out gateReply
		must(t, json.Unmarshal(w.Body.Bytes(), &out))
		return w.Code, out
	}
	if code, _ := call(grant, "permit", "provider-b", "", 0); code != 401 {
		t.Fatal("grant used for another resource", code)
	}
	code, p := call(grant, "permit", "", "", 0)
	if code != 200 || p.Ticket == "" {
		t.Fatal(code, p)
	}
	if code, _ := call(grant, "pause", "", p.Ticket, 7200); code != 200 {
		t.Fatal(code)
	}
	if code, r := call(admin, "permit", "provider-a", "", 0); code != 200 || r.WaitMillis < 7199000 || r.Ticket != "" {
		t.Fatal(code, r)
	}
	if code, r := call(admin, "permit", "provider-b", "", 0); code != 200 || r.Ticket == "" {
		t.Fatal(code, r)
	}
	// Duplicate delivery of the identical overload cannot extend its deadline.
	var expiry string
	must(t, s.Pool.QueryRow(ctx, `select until_at::text from `+s.table("resource_gates")+` where resource='provider-a'`).Scan(&expiry))
	if code, _ := call(grant, "pause", "", p.Ticket, 7200); code != 200 {
		t.Fatal(code)
	}
	var same bool
	must(t, s.Pool.QueryRow(ctx, `select until_at::text=$1 from `+s.table("resource_gates")+` where resource='provider-a'`, expiry).Scan(&same))
	if !same {
		t.Fatal("duplicated pause extended deadline")
	}
	_, e := s.Pool.Exec(ctx, `update `+s.table("resource_gates")+` set until_at=now()-interval '1 second' where resource='provider-a'`)
	must(t, e)
	_, probe := call(grant, "permit", "", "", 0)
	if !probe.Probe {
		t.Fatal(probe)
	}
	_, blocked := call(admin, "permit", "provider-a", "", 0)
	if blocked.Ticket != "" || blocked.WaitMillis <= 0 {
		t.Fatal("more than one probe", blocked)
	}
	if code, _ := call(admin, "release", "provider-a", probe.Ticket, 0); code != 401 {
		t.Fatal("foreign ticket", code)
	}
	if code, _ := call(grant, "release", "", probe.Ticket, 0); code != 200 {
		t.Fatal(code)
	}
	_, again := call(admin, "permit", "provider-a", "", 0)
	if again.Probe || again.Ticket == "" {
		t.Fatal(again)
	}
	_, e = s.Pool.Exec(ctx, `update `+s.table("attempts")+` set lease_until=now()-interval '1 second' where job_id=$1`, response.Job.ID)
	must(t, e)
	if code, _ := call(grant, "pause", "", p.Ticket, 7200); code != 409 {
		t.Fatal("expired grant", code)
	}
	w := request(t, h, "GET", "/v1/admin/workers", grant, "")
	if w.Code != 401 || strings.Contains(fmt.Sprint(w.Body), grant) {
		t.Fatal("resource grant leaked authority")
	}
}
