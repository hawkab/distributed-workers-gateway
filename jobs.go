package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
)

func (s *Store) Claim(ctx context.Context, worker string, capabilities []string) (*ClaimedJob, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	if e = s.lockWorker(ctx, tx, worker); e != nil {
		return nil, e
	}
	_, e = tx.Exec(ctx, `UPDATE `+s.table("workers")+` SET capabilities=$2,last_seen_at=clock_timestamp() WHERE id=$1`, worker, sortedUnique(capabilities))
	if e != nil {
		return nil, e
	}
	var j ClaimedJob
	e = tx.QueryRow(ctx, `SELECT j.id::text,j.operation,j.input,j.script,a.id::text,a.fencing_token,a.lease_until FROM `+s.table("jobs")+` j JOIN `+s.table("attempts")+` a ON a.id=j.active_attempt_id WHERE j.status='running' AND a.worker_id=$1 AND a.status='running' AND a.lease_until>clock_timestamp() ORDER BY a.started_at LIMIT 1`, worker).Scan(&j.ID, &j.Operation, &j.Input, &j.Script, &j.AttemptID, &j.FencingToken, &j.LeaseUntil)
	if e == nil {
		return &j, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return nil, e
	}
	var oldAttempt *string
	e = tx.QueryRow(ctx, `SELECT j.id::text,j.operation,j.input,j.script,j.fencing_token,j.active_attempt_id::text FROM `+s.table("jobs")+` j WHERE NOT j.cancel_requested AND j.not_before<=clock_timestamp() AND j.required_capabilities<@$1::text[] AND (j.status='queued' OR(j.status='running' AND EXISTS(SELECT 1 FROM `+s.table("attempts")+` a WHERE a.id=j.active_attempt_id AND a.lease_until<=clock_timestamp()))) ORDER BY j.priority DESC,j.created_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT 1`, sortedUnique(capabilities)).Scan(&j.ID, &j.Operation, &j.Input, &j.Script, &j.FencingToken, &oldAttempt)
	if errors.Is(e, pgx.ErrNoRows) {
		if e = tx.Commit(ctx); e != nil {
			return nil, e
		}
		return nil, ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	if oldAttempt != nil {
		_, e = tx.Exec(ctx, `UPDATE `+s.table("attempts")+` SET status='expired' WHERE id=$1 AND status='running'`, *oldAttempt)
		if e != nil {
			return nil, e
		}
		if e = s.event(ctx, tx, "attempt.expired", map[string]any{"job_id": j.ID, "attempt_id": *oldAttempt}); e != nil {
			return nil, e
		}
	}
	j.FencingToken++
	j.AttemptID = newID()
	e = tx.QueryRow(ctx, `INSERT INTO `+s.table("attempts")+`(id,job_id,worker_id,fencing_token,lease_until) VALUES($1,$2,$3,$4,clock_timestamp()+$5::double precision*interval '1 second') RETURNING lease_until`, j.AttemptID, j.ID, worker, j.FencingToken, s.leaseSeconds()).Scan(&j.LeaseUntil)
	if e != nil {
		return nil, e
	}
	_, e = tx.Exec(ctx, `UPDATE `+s.table("jobs")+` SET status='running',active_attempt_id=$2,fencing_token=$3,updated_at=clock_timestamp() WHERE id=$1`, j.ID, j.AttemptID, j.FencingToken)
	if e != nil {
		return nil, e
	}
	_, e = tx.Exec(ctx, `UPDATE `+s.table("workers")+` SET state='busy',active_job_id=$2 WHERE id=$1`, worker, j.ID)
	if e != nil {
		return nil, e
	}
	if e = s.event(ctx, tx, "attempt.started", map[string]any{"job_id": j.ID, "attempt_id": j.AttemptID, "worker_id": worker, "fencing_token": j.FencingToken}); e != nil {
		return nil, e
	}
	return &j, tx.Commit(ctx)
}
func (s *Store) JobHeartbeat(ctx context.Context, job string, h JobHeartbeat) (LeaseReceipt, error) {
	var out LeaseReceipt
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	if e = s.lockWorker(ctx, tx, h.WorkerID); e != nil {
		return out, e
	}
	var current *string
	var fence int64
	var status string
	e = tx.QueryRow(ctx, `SELECT active_attempt_id::text,fencing_token,status,cancel_requested FROM `+s.table("jobs")+` WHERE id=$1 FOR UPDATE`, job).Scan(&current, &fence, &status, &out.CancelRequested)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if e != nil {
		return out, e
	}
	if current == nil || *current != h.AttemptID || fence != h.FencingToken || status != "running" {
		return out, ErrLeaseLost
	}
	if len(h.Progress) == 0 {
		h.Progress = json.RawMessage(`{}`)
	}
	e = tx.QueryRow(ctx, `UPDATE `+s.table("attempts")+` SET lease_until=clock_timestamp()+$5::double precision*interval '1 second',last_heartbeat_at=clock_timestamp(),progress=$6 WHERE id=$1 AND job_id=$2 AND worker_id=$3 AND fencing_token=$4 AND status='running' AND lease_until>clock_timestamp() RETURNING lease_until`, h.AttemptID, job, h.WorkerID, h.FencingToken, s.leaseSeconds(), []byte(h.Progress)).Scan(&out.LeaseUntil)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrLeaseLost
	}
	if e != nil {
		return out, e
	}
	_, e = tx.Exec(ctx, `UPDATE `+s.table("workers")+` SET last_seen_at=clock_timestamp(),state='busy',active_job_id=$2 WHERE id=$1`, h.WorkerID, job)
	if e != nil {
		return out, e
	}
	return out, tx.Commit(ctx)
}
func (s *Store) Complete(ctx context.Context, job string, request Complete) (CompletionReceipt, error) {
	out := CompletionReceipt{JobID: job, AttemptID: request.AttemptID}
	c, e := request.normalized()
	if e != nil {
		return out, e
	}
	hash := digest(c)
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	if e = s.lockWorker(ctx, tx, c.WorkerID); e != nil {
		return out, e
	}
	var current *string
	var currentFence int64
	var jobStatus string
	e = tx.QueryRow(ctx, `SELECT active_attempt_id::text,fencing_token,status FROM `+s.table("jobs")+` WHERE id=$1 FOR UPDATE`, job).Scan(&current, &currentFence, &jobStatus)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if e != nil {
		return out, e
	}
	var worker string
	var fence int64
	var live bool
	var previousHash, previousReceipt []byte
	e = tx.QueryRow(ctx, `SELECT worker_id,fencing_token,status='running' AND lease_until>clock_timestamp(),completion_hash,receipt FROM `+s.table("attempts")+` WHERE id=$1 AND job_id=$2 FOR UPDATE`, c.AttemptID, job).Scan(&worker, &fence, &live, &previousHash, &previousReceipt)
	if errors.Is(e, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if e != nil {
		return out, e
	}
	if worker != c.WorkerID || fence != c.FencingToken {
		return out, ErrUnauthorized
	}
	if len(previousReceipt) > 0 {
		if !sameBytes(previousHash, hash) {
			return out, ErrConflict
		}
		if e = json.Unmarshal(previousReceipt, &out); e != nil {
			return out, e
		}
		return out, tx.Commit(ctx)
	}
	out.Committed = current != nil && *current == c.AttemptID && currentFence == c.FencingToken && jobStatus == "running" && live
	if out.Committed {
		_, e = tx.Exec(ctx, `INSERT INTO `+s.table("results")+`(job_id,attempt_id,status,result,error,completion_hash) VALUES($1,$2,$3,$4,$5,$6)`, job, c.AttemptID, c.Status, []byte(c.Result), []byte(c.Error), hash)
		if e != nil {
			return out, e
		}
		_, e = tx.Exec(ctx, `UPDATE `+s.table("jobs")+` SET status=$2,updated_at=clock_timestamp(),finished_at=clock_timestamp() WHERE id=$1`, job, c.Status)
		if e != nil {
			return out, e
		}
	} else {
		out.CandidateSaved = true
		_, e = tx.Exec(ctx, `INSERT INTO `+s.table("candidates")+`(id,job_id,attempt_id,worker_id,status,result,error,completion_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, newID(), job, c.AttemptID, c.WorkerID, c.Status, []byte(c.Result), []byte(c.Error), hash)
		if e != nil {
			return out, e
		}
	}
	receipt, _ := json.Marshal(out)
	status := c.Status
	if !out.Committed {
		status = "stale"
	}
	_, e = tx.Exec(ctx, `UPDATE `+s.table("attempts")+` SET status=$2,completed_at=clock_timestamp(),completion_hash=$3,receipt=$4 WHERE id=$1`, c.AttemptID, status, hash, receipt)
	if e != nil {
		return out, e
	}
	// Do not clear a newer attempt that the same device has already claimed.
	_, e = tx.Exec(ctx, `UPDATE `+s.table("workers")+` SET last_seen_at=clock_timestamp(),state=CASE WHEN NOT EXISTS(SELECT 1 FROM `+s.table("attempts")+` a WHERE a.worker_id=$1 AND a.status='running' AND a.lease_until>clock_timestamp()) THEN 'idle' ELSE state END,active_job_id=CASE WHEN NOT EXISTS(SELECT 1 FROM `+s.table("attempts")+` a WHERE a.worker_id=$1 AND a.status='running' AND a.lease_until>clock_timestamp()) THEN '' ELSE active_job_id END WHERE id=$1`, c.WorkerID)
	if e != nil {
		return out, e
	}
	event := "job." + c.Status
	if !out.Committed {
		event = "result.candidate_saved"
	}
	if e = s.event(ctx, tx, event, map[string]any{"job_id": job, "attempt_id": c.AttemptID, "status": c.Status, "committed": out.Committed, "candidate_saved": out.CandidateSaved, "result_ref": "/v1/admin/jobs/" + job}); e != nil {
		return out, e
	}
	return out, tx.Commit(ctx)
}
func (s *Store) Cancel(ctx context.Context, job string) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	tag, e := tx.Exec(ctx, `UPDATE `+s.table("jobs")+` SET cancel_requested=true,status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,updated_at=clock_timestamp() WHERE id=$1 AND status IN('queued','running')`, job)
	if e != nil {
		return e
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if e = s.event(ctx, tx, "job.cancel_requested", map[string]string{"job_id": job}); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
