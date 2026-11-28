package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type gatePolicy struct {
	Resource   string `json:"resource"`
	InputField string `json:"input_field"`
}

var gateResource = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9.:/_-]{0,199}$`)
var gateField = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,63}$`)

func gateFromNetwork(raw json.RawMessage) (*gatePolicy, error) {
	var policy struct {
		Gate *gatePolicy `json:"resource_gate"`
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	if e := json.Unmarshal(raw, &policy); e != nil {
		return nil, ErrInvalid
	}
	if g := policy.Gate; g != nil && (!gateResource.MatchString(g.Resource) || !gateField.MatchString(g.InputField)) {
		return nil, ErrInvalid
	}
	return policy.Gate, nil
}

func (s *Store) initResourceGate(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+s.table("resource_gates")+`(resource text PRIMARY KEY, until_at timestamptz NOT NULL, probe uuid, probe_until timestamptz);
 CREATE TABLE IF NOT EXISTS `+s.table("resource_gate_reports")+`(id uuid PRIMARY KEY, until_at timestamptz NOT NULL)`)
	return err
}

type gateAuthority struct {
	Job      string `json:"j"`
	Attempt  string `json:"a"`
	Fence    int64  `json:"f"`
	Resource string `json:"r"`
}

func (a *API) gateMAC(purpose, value string) string {
	mac := hmac.New(sha256.New, []byte(a.AdminToken))
	mac.Write([]byte("resource-gate/v1:" + purpose + "\x00" + value))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// The producer's stored job and its hash remain immutable. Inject authority only
// in the response to the authenticated owner. A superseded attempt is rejected.
func (a *API) deliverJob(w http.ResponseWriter, job *ClaimedJob) {
	g, e := gateFromNetwork(job.Network)
	if e != nil {
		fail(w, e)
		return
	}
	if g != nil {
		var wrapper, payload map[string]json.RawMessage
		if a.AdminToken == "" || json.Unmarshal(job.Input, &wrapper) != nil || json.Unmarshal(wrapper[g.InputField], &payload) != nil || payload == nil {
			fail(w, ErrInvalid)
			return
		}
		data, _ := json.Marshal(gateAuthority{job.ID, job.AttemptID, job.FencingToken, g.Resource})
		encoded := base64.RawURLEncoding.EncodeToString(data)
		payload["resource_gate_grant"], _ = json.Marshal(encoded + "." + a.gateMAC("grant", encoded))
		wrapper[g.InputField], _ = json.Marshal(payload)
		job.Input, _ = json.Marshal(wrapper)
		// resource_gate controls the producer's coordination channel. Only
		// actual network restrictions belong in the worker sandbox policy.
		var network map[string]json.RawMessage
		if json.Unmarshal(job.Network, &network) != nil {
			fail(w, ErrInvalid)
			return
		}
		delete(network, "resource_gate")
		job.Network = nil
		if len(network) > 0 {
			job.Network, _ = json.Marshal(network)
		}
	}
	writeJSON(w, 200, map[string]any{"job": job})
}

type gateCommand struct {
	Action     string `json:"action"`
	RTTMillis  int64  `json:"rtt_ms,omitempty"`
	Resource   string `json:"resource,omitempty"`
	Ticket     string `json:"ticket,omitempty"`
	RetryAfter int64  `json:"retry_after_seconds,omitempty"`
}

type gateReply struct {
	Ticket      string `json:"ticket,omitempty"`
	ValidMillis int64  `json:"valid_ms,omitempty"`
	WaitMillis  int64  `json:"wait_ms,omitempty"`
	Probe       bool   `json:"probe,omitempty"`
}

func (a *API) workerResourceGate(w http.ResponseWriter, r *http.Request) {
	grant := bearer(r)
	encoded, signature, ok := strings.Cut(grant, ".")
	if !ok || len(encoded) > 1000 || a.AdminToken == "" || !hmac.Equal([]byte(signature), []byte(a.gateMAC("grant", encoded))) {
		fail(w, ErrUnauthorized)
		return
	}
	b, e := base64.RawURLEncoding.DecodeString(encoded)
	var auth gateAuthority
	if e != nil || json.Unmarshal(b, &auth) != nil || !uuidPattern.MatchString(auth.Job) || !uuidPattern.MatchString(auth.Attempt) || !gateResource.MatchString(auth.Resource) {
		fail(w, ErrUnauthorized)
		return
	}
	var cmd gateCommand
	if !a.decode(w, r, &cmd) {
		return
	}
	if cmd.Resource != "" && cmd.Resource != auth.Resource {
		fail(w, ErrUnauthorized)
		return
	}
	cmd.Resource = auth.Resource
	a.resourceGate(w, r, cmd, &auth, signature)
}

func (a *API) adminResourceGate(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	var cmd gateCommand
	if !a.decode(w, r, &cmd) {
		return
	}
	a.resourceGate(w, r, cmd, nil, "admin")
}

func (a *API) resourceGate(w http.ResponseWriter, r *http.Request, cmd gateCommand, auth *gateAuthority, scope string) {
	if !gateResource.MatchString(cmd.Resource) || (cmd.Action != "permit" && cmd.Action != "pause" && cmd.Action != "release" && cmd.Action != "abandon") {
		fail(w, ErrInvalid)
		return
	}
	// time.Duration bounds the client wire protocol; no provider-stated delay is
	// clamped to an operational polling interval.
	if cmd.Action == "pause" && (cmd.RetryAfter < 1 || cmd.RetryAfter > int64((time.Duration(1<<63-1))/time.Second)) {
		fail(w, ErrInvalid)
		return
	}
	id := ""
	if cmd.Action != "permit" {
		var sig string
		var ok bool
		id, sig, ok = strings.Cut(cmd.Ticket, ".")
		if !ok || !uuidPattern.MatchString(id) || !hmac.Equal([]byte(a.gateMAC("ticket", scope+"|"+cmd.Resource+"|"+id)), []byte(sig)) {
			fail(w, ErrUnauthorized)
			return
		}
	}
	ctx := r.Context()
	tx, err := a.Store.Pool.Begin(ctx)
	if err != nil {
		fail(w, err)
		return
	}
	defer tx.Rollback(ctx)
	if auth != nil {
		var active bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+a.Store.table("jobs")+` j JOIN `+a.Store.table("attempts")+` a ON a.id=j.active_attempt_id JOIN `+a.Store.table("workers")+` w ON w.id=a.worker_id WHERE j.id=$1 AND a.id=$2 AND j.fencing_token=$3 AND a.fencing_token=$3 AND a.status='running' AND a.lease_until>clock_timestamp() AND j.status='running' AND w.enabled AND NOT j.cancel_requested)`, auth.Job, auth.Attempt, auth.Fence).Scan(&active)
		if err != nil {
			fail(w, err)
			return
		}
		if !active {
			fail(w, ErrLeaseLost)
			return
		}
	}
	reply := gateReply{}
	table := a.Store.table("resource_gates")
	switch cmd.Action {
	case "permit":
		id = newID()
		var wait, probeWait float64
		// In the normal path this is an indexed read and does not create a row.
		err = tx.QueryRow(ctx, `SELECT extract(epoch from until_at-clock_timestamp())::float8,coalesce(extract(epoch from probe_until-clock_timestamp())::float8,0) FROM `+table+` WHERE resource=$1 FOR UPDATE`, cmd.Resource).Scan(&wait, &probeWait)
		if errors.Is(err, pgx.ErrNoRows) {
			err = nil
		} else if err == nil {
			if seconds := max(wait, probeWait); seconds > 0 {
				reply.WaitMillis = max(1, int64(seconds*1000)+1)
			} else {
				_, err = tx.Exec(ctx, `UPDATE `+table+` SET probe=$2,probe_until=clock_timestamp()+$3*interval '1 second' WHERE resource=$1`, cmd.Resource, id, a.Store.leaseSeconds())
				reply.Probe = true
			}
		}
		if reply.WaitMillis == 0 {
			reply.Ticket = id + "." + a.gateMAC("ticket", scope+"|"+cmd.Resource+"|"+id)
			// Validity bounds how late a client may start a request after permission.
			// Slow permission delivery is renewed without expiring the job itself.
			reply.ValidMillis = max(2000, 2*min(int64(a.Store.leaseSeconds()*1000), max(0, cmd.RTTMillis)))
		}
	case "pause":
		receipts := a.Store.table("resource_gate_reports")
		_, err = tx.Exec(ctx, `WITH once AS (INSERT INTO `+receipts+` (id,until_at) VALUES($1,clock_timestamp()+$3::bigint*interval '1 second') ON CONFLICT DO NOTHING RETURNING until_at)
  INSERT INTO `+table+` AS g(resource,until_at) SELECT $2,until_at FROM once ON CONFLICT(resource) DO UPDATE SET until_at=greatest(g.until_at,excluded.until_at),probe=NULL,probe_until=NULL`, id, cmd.Resource, cmd.RetryAfter)
	case "abandon":
		_, err = tx.Exec(ctx, `UPDATE `+table+` SET probe=NULL,probe_until=NULL WHERE resource=$1 AND probe=$2`, cmd.Resource, id)
	case "release":
		_, err = tx.Exec(ctx, `DELETE FROM `+table+` WHERE resource=$1 AND probe=$2`, cmd.Resource, id)
	}
	if err != nil {
		fail(w, err)
		return
	}
	if err = tx.Commit(ctx); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, reply)
}
