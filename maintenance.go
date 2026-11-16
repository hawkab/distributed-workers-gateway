package gateway

import (
	"context"
	"time"
)

// Reconciliation ends only explicitly cancelled jobs whose worker lease has
// expired. A healthy long-running job is never stopped by its age.
func (s *Store) ReconcileCancelled(ctx context.Context) error {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, `SELECT j.id::text FROM `+s.table("jobs")+` j WHERE j.status='running' AND j.cancel_requested AND NOT EXISTS(SELECT 1 FROM `+s.table("attempts")+` a WHERE a.id=j.active_attempt_id AND a.status='running' AND a.lease_until>clock_timestamp()) FOR UPDATE OF j SKIP LOCKED`)
	if e != nil {
		return e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		_, e = tx.Exec(ctx, `UPDATE `+s.table("jobs")+` SET status='cancelled',finished_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1`, id)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `UPDATE `+s.table("attempts")+` SET status='cancelled' WHERE job_id=$1 AND status='running'`, id)
		if e != nil {
			return e
		}
		if e = s.event(ctx, tx, "job.cancelled", map[string]string{"job_id": id}); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) RunMaintenance(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ReconcileCancelled(ctx)
		}
	}
}
