package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestMixedSmallWaitAndOlderClients(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "inline")
	types := []string{"small", "large", "legacy", "exponent", "failed", "cancelled"}
	ids := make([]string, 0, len(types))
	individual := map[string]bool{}
	for _, kind := range types {
		id := enqueue(t, s, "sha256", kind)
		ids = append(ids, id)
		if kind == "cancelled" {
			must(t, s.Cancel(ctx, id))
			continue
		}
		j, e := s.Claim(ctx, "inline", []string{"sha256"})
		must(t, e)
		c := done(j, "inline")
		switch kind {
		case "small":
			c.Result = json.RawMessage(`{"markup":"<&>","quoted":"1e20000","unicode":"ёж"}`)
			individual[id] = true
		case "large":
			c.Result, _ = json.Marshal(strings.Repeat("z", 90000))
		case "legacy": // This row models pre-upgrade canonical results.
		case "exponent":
			c.Result = json.RawMessage(`{"n":1e10000}`)
		case "failed":
			c.Status = "failed"
			c.Result = json.RawMessage("null")
			c.Error = json.RawMessage(`{"code":"fixture"}`)
			individual[id] = true
		}
		_, e = s.Complete(ctx, id, c)
		must(t, e)
		if kind == "legacy" {
			_, e = s.Pool.Exec(ctx, "UPDATE "+s.table("results")+" SET wait_inline_bytes=NULL WHERE job_id=$1", id)
			must(t, e)
		}
	}
	h := (&API{Store: s, AdminToken: "operator"}).Handler()
	req := func(method, url, prefer string, body []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, url, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer operator")
		r.Header.Set("Prefer", prefer)
		v := httptest.NewRecorder()
		h.ServeHTTP(v, r)
		if v.Code != 200 {
			t.Fatal("status", v.Code, v.Body.String())
		}
		return v
	}
	b, _ := json.Marshal(map[string]any{"ids": ids})
	for _, mode := range []string{"return=minimal, include=small-results", "return=minimal", "", "include=small-results"} {
		v := req("POST", "/v1/admin/jobs/wait", mode, b)
		var reply struct{ Jobs []map[string]any }
		dec := json.NewDecoder(v.Body)
		dec.UseNumber()
		must(t, dec.Decode(&reply))
		if len(reply.Jobs) != len(ids) {
			t.Fatal(len(reply.Jobs))
		}
		for _, row := range reply.Jobs {
			id := row["id"].(string)
			_, has := row["result"]
			want := mode == "" || mode == "include=small-results" || (mode == "return=minimal, include=small-results" && individual[id])
			if want != has {
				t.Fatalf("mode %s kind %s has result %t", mode, id, has)
			}
			if has {
				res := req("GET", "/v1/admin/jobs/"+id+"/result", "", nil)
				if individual[id] {
					var other map[string]any
					dec := json.NewDecoder(res.Body)
					dec.UseNumber()
					must(t, dec.Decode(&other))
					if !reflect.DeepEqual(row, other) {
						t.Fatal("individual result differed")
					}
				}
			} else if row["result_ref"] != "/v1/admin/jobs/"+id+"/result" {
				t.Fatal("lost fallback")
			}
		}
	}
}
func TestSmallWaitResponseBoundAndContinuation(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "many")
	// Populate full and missing-size rows in one statement. Real Complete's
	// size calculation is checked separately above.
	rows, err := s.Pool.Query(ctx, `WITH j AS (INSERT INTO `+s.table("jobs")+`(id,operation,input,status,request_hash) SELECT gen_random_uuid(),'sha256','{}','succeeded','x' FROM generate_series(1,$1) RETURNING id),
 a AS (INSERT INTO `+s.table("attempts")+`(id,job_id,worker_id,fencing_token,status,lease_until) SELECT gen_random_uuid(),id,'many',1,'succeeded',now() FROM j RETURNING id,job_id)
 INSERT INTO `+s.table("results")+`(job_id,attempt_id,status,result,completion_hash,wait_inline_bytes)
 SELECT job_id,id,'succeeded',to_jsonb(repeat('<',1470)),'x',octet_length((`+waitResultJSON("job_id", "'succeeded'", "to_jsonb(repeat('<',1470))", "NULL")+`)::text) FROM a RETURNING job_id::text`, waitMaxRefRows+35)
	must(t, err)
	var ids []string
	for rows.Next() {
		var id string
		must(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	must(t, rows.Err())
	rows.Close()
	h := (&API{Store: s, AdminToken: "operator"}).Handler()
	seen := map[string]bool{}
	refs, inline, pages := 0, 0, 0
	for len(ids) > 0 {
		payload, _ := json.Marshal(map[string]any{"ids": ids})
		r := httptest.NewRequest(http.MethodPost, "/v1/admin/jobs/wait", bytes.NewReader(payload))
		r.Header.Set("Authorization", "Bearer operator")
		r.Header.Set("Prefer", "return=minimal, include=small-results")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 || w.Body.Len() > waitResponseBudgetBytes {
			t.Fatal(w.Code, w.Body.Len())
		}
		var result struct {
			Jobs []struct {
				ID     string
				Ref    string `json:"result_ref"`
				Result json.RawMessage
			}
			More bool `json:"more_ready"`
		}
		must(t, json.Unmarshal(w.Body.Bytes(), &result))
		if len(result.Jobs) == 0 || result.More != (len(result.Jobs) < len(ids)) {
			t.Fatal("bad pagination")
		}
		for _, row := range result.Jobs {
			if seen[row.ID] {
				t.Fatal("duplicate page")
			}
			seen[row.ID] = true
			if row.Ref != "" {
				refs++
			} else if len(row.Result) > 0 {
				inline++
			} else {
				t.Fatal("missing body and ref")
			}
		}
		next := ids[:0]
		for _, id := range ids {
			if !seen[id] {
				next = append(next, id)
			}
		}
		ids = next
		pages++
	}
	if pages != 2 || refs == 0 || inline == 0 {
		t.Fatal(fmt.Sprint(pages, refs, inline))
	}
}
