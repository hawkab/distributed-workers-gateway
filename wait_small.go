package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// A native receipt fit below 2 KiB at p95. Reserve a fixed bound for all
// references before choosing which results to inline. Earlier deployments and
// compressed large JSON never need to be read to service this projection.
const (
	waitInlineResultBytes    = 2 << 10
	waitResponseBudgetBytes  = 64 << 10
	waitEnvelopeReserveBytes = 32
	waitRefReserveBytes      = 160 // PostgreSQL reference JSON, UUIDs, status and ", ".
	waitMaxRefRows           = (waitResponseBudgetBytes - waitEnvelopeReserveBytes) / waitRefReserveBytes
	waitRefJSON              = `jsonb_build_object('id',j.id,'status',j.status,'result_ref','/v1/admin/jobs/'||j.id::text||'/result')`
)

func prefers(headers []string, value string) bool {
	for _, header := range headers {
		for _, item := range strings.Split(header, ",") {
			token, _, _ := strings.Cut(item, ";")
			if strings.EqualFold(strings.TrimSpace(token), value) {
				return true
			}
		}
	}
	return false
}

func waitResultJSON(id, status, result, failure string) string {
	return `jsonb_build_object('id',` + id + `,'status',` + status + `,'result',jsonb_build_object('status',` + status + `,'result',` + result + `,'error',` + failure + `))`
}

// PostgreSQL expands JSON numbers with exponents when rendering jsonb as text.
// Even a short input can consequently be large. Leave those values on the
// reference path without constructing their expanded wire representation.
func canMeasureWaitResult(result, failure json.RawMessage) bool {
	if len(result)+len(failure) > waitInlineResultBytes {
		return false
	}
	for _, raw := range []json.RawMessage{result, failure} {
		quoted := false
		for i := 0; i < len(raw); i++ {
			switch raw[i] {
			case '\\':
				if quoted {
					i++
				}
			case '"':
				quoted = !quoted
			case 'e', 'E':
				if !quoted && i > 0 && raw[i-1] >= '0' && raw[i-1] <= '9' {
					return false
				}
			}
		}
	}
	return true
}

func (s *Store) smallResultsWaitSQL() string {
	// Only metadata is projected into either window. A scalar subquery in
	// the selected CASE arm reads payload columns only for admitted rows.
	return `WITH ready AS MATERIALIZED (
        SELECT j.id,j.status,r.wait_inline_bytes
        FROM ` + s.table("jobs") + ` j LEFT JOIN ` + s.table("results") + ` r ON r.job_id=j.id
        WHERE j.id=ANY($1::uuid[]) AND j.status IN('succeeded','failed','cancelled')
        ORDER BY j.id LIMIT ($2::integer+1)
    ), budget AS (
        SELECT *, row_number() OVER(ORDER BY id) AS row_no, count(*) OVER() AS ready_count,
            sum(greatest(coalesce(wait_inline_bytes,0)+2-$3::integer,0)) OVER(ORDER BY id) AS extra
        FROM ready
    )
    SELECT coalesce(jsonb_agg(CASE WHEN j.wait_inline_bytes IS NOT NULL
        AND $4::integer+least(j.ready_count,$2::integer)*$3::integer+j.extra <= $5::integer
        THEN (SELECT ` + waitResultJSON("j.id", "j.status", "r.result", "r.error") + ` FROM ` + s.table("results") + ` r WHERE r.job_id=j.id)
        ELSE ` + waitRefJSON + ` END ORDER BY j.id),'[]'::jsonb),
        coalesce(bool_or(j.ready_count>$2::integer),false)
    FROM budget j WHERE j.row_no<=$2::integer`
}

func writeSmallWait(w http.ResponseWriter, jobs []byte, more bool) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, `{"jobs":`)
	w.Write(jobs)
	if more {
		io.WriteString(w, `,"more_ready":true`)
	}
	io.WriteString(w, "}\n")
}
