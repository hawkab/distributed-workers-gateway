package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/url"
	"regexp"
	"strings"
)

var headerToken = regexp.MustCompile("^[!#$%&'*+.^_\\x60|~0-9A-Za-z-]+$")

type normalizedEffectRequest struct {
	sha, method, policy, header string
	keyHash                     []byte
}

func normalizeEffectRequest(raw json.RawMessage) (normalizedEffectRequest, error) {
	var out normalizedEffectRequest
	var request map[string]json.RawMessage
	if json.Unmarshal(raw, &request) != nil || request == nil {
		return out, ErrInvalid
	}
	get := func(key, def string) (string, error) {
		value, ok := request[key]
		if !ok {
			return def, nil
		}
		var s string
		if json.Unmarshal(value, &s) != nil {
			return "", ErrInvalid
		}
		return s, nil
	}
	method, e := get("method", "GET")
	if e != nil {
		return out, e
	}
	method = strings.ToUpper(method)
	if !headerToken.MatchString(method) {
		return out, ErrInvalid
	}
	address, e := get("url", "")
	if e != nil {
		return out, e
	}
	u, e := url.Parse(address)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return out, ErrInvalid
	}
	policyDefault := "never"
	if method == "GET" || method == "HEAD" {
		policyDefault = "safe"
	}
	policy, e := get("replay", policyDefault)
	if e != nil {
		return out, e
	}
	if policy != "safe" && policy != "idempotent" && policy != "never" {
		return out, ErrInvalid
	}
	if policy == "safe" && method != "GET" && method != "HEAD" {
		return out, ErrInvalid
	}
	headers := map[string][]string{}
	var original map[string]json.RawMessage
	if value, ok := request["headers"]; ok {
		if json.Unmarshal(value, &original) != nil || original == nil {
			return out, ErrInvalid
		}
	}
	for name, value := range original {
		if !headerToken.MatchString(name) {
			return out, ErrInvalid
		}
		lower := strings.ToLower(name)
		if _, ok := headers[lower]; ok {
			return out, ErrInvalid
		}
		var values []string
		var single string
		if json.Unmarshal(value, &single) == nil {
			values = []string{single}
		} else if json.Unmarshal(value, &values) != nil || values == nil {
			return out, ErrInvalid
		}
		for _, v := range values {
			if strings.ContainsAny(v, "\r\n\x00") {
				return out, ErrInvalid
			}
		}
		headers[lower] = values
	}
	for name, value := range map[string]string{"accept": "*/*", "accept-encoding": "identity", "user-agent": "DistributedWorker/1"} {
		if _, ok := headers[name]; !ok {
			headers[name] = []string{value}
		}
	}
	header := ""
	var keyHash []byte
	if policy == "idempotent" {
		header, e = get("idempotency_header", "Idempotency-Key")
		if e != nil || !headerToken.MatchString(header) {
			return out, ErrInvalid
		}
		header = strings.ToLower(header)
		key, e := get("idempotency_key", "")
		if e != nil || key == "" {
			return out, ErrInvalid
		}
		actual := headers[header]
		if len(actual) != 1 || actual[0] != key {
			return out, ErrInvalid
		}
		keyHash = tokenHash(key)
		request["idempotency_header"], _ = json.Marshal(header)
	}
	request["method"], _ = json.Marshal(method)
	request["replay"], _ = json.Marshal(policy)
	request["headers"], _ = json.Marshal(headers)
	if _, ok := request["body_text"]; !ok {
		request["body_text"] = json.RawMessage(`""`)
	} else {
		if _, e = get("body_text", ""); e != nil {
			return out, e
		}
	}
	if _, ok := request["response"]; !ok {
		request["response"] = json.RawMessage(`{"mode":"artifact"}`)
	}
	canonicalBytes, e := json.Marshal(request)
	if e != nil {
		return out, ErrInvalid
	}
	canonicalBytes, e = canonical(canonicalBytes)
	if e != nil {
		return out, e
	}
	sum := sha256.Sum256(canonicalBytes)
	return normalizedEffectRequest{sha: hex.EncodeToString(sum[:]), method: method, policy: policy, header: header, keyHash: keyHash}, nil
}

type effectRow struct {
	sha, policy, method, header, worker, attempt, execution, status string
	fence                                                           int64
	keyHash, response, finishHash, receipt                          []byte
	artifactIDs                                                     []string
}

func (s *Store) recordExecution(ctx context.Context, tx pgx.Tx, job, key string, c EffectBegin, sha string) error {
	tag, e := tx.Exec(ctx, `INSERT INTO `+s.table("effect_executions")+`(job_id,effect_id,execution_id,worker_id,attempt_id,fencing_token,request_sha256) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, job, key, c.ExecutionID, c.WorkerID, c.AttemptID, c.FencingToken, sha)
	if e != nil {
		return e
	}
	// A retired nonce cannot reclaim ownership after a newer safe/idempotent
	// execution. Current-nonce RPC retries return before this insertion.
	if tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}
func (s *Store) effect(ctx context.Context, tx pgx.Tx, job, key string) (effectRow, error) {
	var r effectRow
	e := tx.QueryRow(ctx, `SELECT request_sha256,replay_policy,method,idempotency_header,idempotency_key_hash,worker_id,attempt_id::text,fencing_token,execution_id::text,status,response,artifact_ids,finish_hash,receipt FROM `+s.table("effects")+` WHERE job_id=$1 AND effect_id=$2 FOR UPDATE`, job, key).Scan(&r.sha, &r.policy, &r.method, &r.header, &r.keyHash, &r.worker, &r.attempt, &r.fence, &r.execution, &r.status, &r.response, &r.artifactIDs, &r.finishHash, &r.receipt)
	return r, e
}
func (s *Store) BeginEffect(ctx context.Context, job, key string, c EffectBegin) (EffectReceipt, error) {
	var out EffectReceipt
	if !uuidPattern.MatchString(job) || !validID(key) || !c.AttemptIdentity.valid() || !uuidPattern.MatchString(c.ExecutionID) {
		return out, ErrInvalid
	}
	var original map[string]json.RawMessage
	if json.Unmarshal(c.Request, &original) != nil || original == nil {
		return out, ErrInvalid
	}
	if declared, ok := original["effect_id"]; ok {
		var id string
		if json.Unmarshal(declared, &id) != nil || id != key {
			return out, ErrInvalid
		}
	}
	original["effect_id"], _ = json.Marshal(key)
	normalizedOriginal, _ := json.Marshal(original)
	n, e := normalizeEffectRequest(normalizedOriginal)
	if e != nil {
		return out, e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	live, e := s.attemptState(ctx, tx, job, c.AttemptIdentity)
	if e != nil {
		return out, e
	}
	r, e := s.effect(ctx, tx, job, key)
	out = EffectReceipt{EffectID: key, ExecutionID: c.ExecutionID, RequestSHA256: n.sha}
	if errors.Is(e, pgx.ErrNoRows) {
		if !live {
			return out, ErrLeaseLost
		}
		_, e = tx.Exec(ctx, `INSERT INTO `+s.table("effects")+`(job_id,effect_id,request_sha256,replay_policy,method,idempotency_header,idempotency_key_hash,worker_id,attempt_id,fencing_token,execution_id,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'started')`, job, key, n.sha, n.policy, n.method, n.header, n.keyHash, c.WorkerID, c.AttemptID, c.FencingToken, c.ExecutionID)
		if e != nil {
			return out, e
		}
		if e = s.recordExecution(ctx, tx, job, key, c, n.sha); e != nil {
			return out, e
		}
		out.State = "execute"
		if e = s.event(ctx, tx, "effect.started", map[string]any{"job_id": job, "effect_id": key, "execution_id": c.ExecutionID}); e != nil {
			return out, e
		}
		return out, tx.Commit(ctx)
	}
	if e != nil {
		return out, e
	}
	if r.sha != n.sha {
		return out, ErrConflict
	}
	if r.status == "completed" {
		out.State = "completed"
		out.ExecutionID = r.execution
		out.Response = json.RawMessage(r.response)
		out.ArtifactIDs = r.artifactIDs
		return out, tx.Commit(ctx)
	}
	same := r.execution == c.ExecutionID && r.worker == c.WorkerID && r.attempt == c.AttemptID && r.fence == c.FencingToken
	if same && r.status == "started" {
		if !live {
			return out, ErrLeaseLost
		}
		out.State = "execute"
		return out, tx.Commit(ctx)
	}
	if r.policy == "never" {
		out.State = "uncertain"
		out.ExecutionID = r.execution
		if r.status != "uncertain" {
			_, e = tx.Exec(ctx, `UPDATE `+s.table("effects")+` SET status='uncertain',updated_at=clock_timestamp() WHERE job_id=$1 AND effect_id=$2`, job, key)
			if e != nil {
				return out, e
			}
			if e = s.event(ctx, tx, "effect.uncertain", map[string]any{"job_id": job, "effect_id": key, "execution_id": r.execution}); e != nil {
				return out, e
			}
		}
		return out, tx.Commit(ctx)
	}
	if !live {
		return out, ErrLeaseLost
	}
	if e = s.recordExecution(ctx, tx, job, key, c, n.sha); e != nil {
		return out, e
	}
	_, e = tx.Exec(ctx, `UPDATE `+s.table("effects")+` SET worker_id=$3,attempt_id=$4,fencing_token=$5,execution_id=$6,status='started',updated_at=clock_timestamp() WHERE job_id=$1 AND effect_id=$2`, job, key, c.WorkerID, c.AttemptID, c.FencingToken, c.ExecutionID)
	if e != nil {
		return out, e
	}
	out.State = "execute"
	if e = s.event(ctx, tx, "effect.started", map[string]any{"job_id": job, "effect_id": key, "execution_id": c.ExecutionID}); e != nil {
		return out, e
	}
	return out, tx.Commit(ctx)
}
func (s *Store) FinishEffect(ctx context.Context, job, key string, c EffectFinish) (EffectReceipt, error) {
	out := EffectReceipt{EffectID: key, ExecutionID: c.ExecutionID, Recorded: true}
	if !validID(key) || !uuidPattern.MatchString(job) || !c.AttemptIdentity.valid() || !uuidPattern.MatchString(c.ExecutionID) || !validSHA(c.RequestSHA256) {
		return out, ErrInvalid
	}
	var response map[string]json.RawMessage
	if json.Unmarshal(c.Response, &response) != nil || response == nil {
		return out, ErrInvalid
	}
	var e error
	c.Response, e = canonical(c.Response)
	if e != nil {
		return out, e
	}
	c.ArtifactIDs = sortedUnique(c.ArtifactIDs)
	for _, id := range c.ArtifactIDs {
		if !uuidPattern.MatchString(id) {
			return out, ErrInvalid
		}
	}
	hash := digest(c)
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	if _, e = s.attemptState(ctx, tx, job, c.AttemptIdentity); e != nil {
		return out, e
	}
	r, e := s.effect(ctx, tx, job, key)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if e != nil {
		return out, e
	}
	if r.sha != c.RequestSHA256 {
		return out, ErrConflict
	}
	var authorized bool
	e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+s.table("effect_executions")+` WHERE job_id=$1 AND effect_id=$2 AND execution_id=$3 AND worker_id=$4 AND attempt_id=$5 AND fencing_token=$6 AND request_sha256=$7)`, job, key, c.ExecutionID, c.WorkerID, c.AttemptID, c.FencingToken, c.RequestSHA256).Scan(&authorized)
	if e != nil {
		return out, e
	}
	if !authorized {
		return out, ErrUnauthorized
	}
	if e = s.validateArtifacts(ctx, tx, job, c.ArtifactIDs); e != nil {
		return out, e
	}
	owner := r.worker == c.WorkerID && r.attempt == c.AttemptID && r.fence == c.FencingToken && r.execution == c.ExecutionID
	if r.status == "completed" && owner {
		if !sameBytes(r.finishHash, hash) {
			return out, ErrConflict
		}
		if e = json.Unmarshal(r.receipt, &out); e != nil {
			return out, e
		}
		return out, tx.Commit(ctx)
	}
	if !owner || r.status == "completed" {
		out.CandidateSaved = true
		tag, e := tx.Exec(ctx, `INSERT INTO `+s.table("effect_candidates")+`(job_id,effect_id,attempt_id,content_hash,response,artifact_ids) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, job, key, c.AttemptID, hash, []byte(c.Response), c.ArtifactIDs)
		if e != nil {
			return out, e
		}
		if tag.RowsAffected() > 0 {
			if e = s.event(ctx, tx, "effect.candidate_saved", map[string]any{"job_id": job, "effect_id": key, "attempt_id": c.AttemptID, "execution_id": c.ExecutionID}); e != nil {
				return out, e
			}
		}
		return out, tx.Commit(ctx)
	}
	out.Committed = true
	receipt, _ := json.Marshal(out)
	_, e = tx.Exec(ctx, `UPDATE `+s.table("effects")+` SET status='completed',response=$3,artifact_ids=$4,finish_hash=$5,receipt=$6,updated_at=clock_timestamp() WHERE job_id=$1 AND effect_id=$2`, job, key, []byte(c.Response), c.ArtifactIDs, hash, receipt)
	if e != nil {
		return out, e
	}
	if e = s.event(ctx, tx, "effect.completed", map[string]any{"job_id": job, "effect_id": key, "execution_id": c.ExecutionID, "effect_ref": "/v1/jobs/" + job + "/effects/" + key + "/status"}); e != nil {
		return out, e
	}
	return out, tx.Commit(ctx)
}
func (s *Store) EffectStatus(ctx context.Context, job, key string, i AttemptIdentity) (EffectReceipt, error) {
	var out EffectReceipt
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	if _, e = s.attemptState(ctx, tx, job, i); e != nil {
		return out, e
	}
	r, e := s.effect(ctx, tx, job, key)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if e != nil {
		return out, e
	}
	out = EffectReceipt{EffectID: key, ExecutionID: r.execution, RequestSHA256: r.sha, State: r.status}
	if r.status == "completed" {
		out.Response = json.RawMessage(r.response)
		out.ArtifactIDs = r.artifactIDs
	}
	return out, tx.Commit(ctx)
}
