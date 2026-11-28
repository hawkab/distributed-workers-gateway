package gateway

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

type WithdrawalReceipt struct {
	JobID       string `json:"job_id"`
	OperationID string `json:"operation_id"`
	Withdrawn   bool   `json:"withdrawn"`
}

// WithdrawUnclaimed uses the same job-row lock as Claim. Success proves that
// no attempt ever held this job. A retry can only replay its original receipt.
func (s *Store) WithdrawUnclaimed(ctx context.Context, job, operation string) (WithdrawalReceipt, error) {
	var empty WithdrawalReceipt
	if !uuidPattern.MatchString(job) || !uuidPattern.MatchString(operation) {
		return empty, ErrInvalid
	}
	receipt := WithdrawalReceipt{JobID: strings.ToLower(job), OperationID: strings.ToLower(operation), Withdrawn: true}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback(ctx)
	var eligible bool
	err = tx.QueryRow(ctx, `SELECT status='queued' AND NOT cancel_requested AND fencing_token=0 AND active_attempt_id IS NULL
   FROM `+s.table("jobs")+` WHERE id=$1 FOR UPDATE`, job).Scan(&eligible)
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, ErrNotFound
	}
	if err != nil {
		return empty, err
	}
	if !eligible {
		var recorded bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+s.table("unclaimed_withdrawals")+` WHERE job_id=$1 AND operation_id=$2)`, job, operation).Scan(&recorded)
		if err != nil {
			return empty, err
		}
		if !recorded {
			return empty, ErrConflict
		}
		return receipt, tx.Commit(ctx)
	}
	// The attempt index starts with job_id. This also protects legacy rows whose
	// current state and fence were reset without deleting historical attempts.
	tag, err := tx.Exec(ctx, `INSERT INTO `+s.table("unclaimed_withdrawals")+` (job_id,operation_id)
      SELECT $1::uuid,$2::uuid WHERE NOT EXISTS(SELECT 1 FROM `+s.table("attempts")+` WHERE job_id=$1)
      ON CONFLICT DO NOTHING`, job, operation)
	if err != nil {
		return empty, err
	}
	if tag.RowsAffected() != 1 {
		return empty, ErrConflict
	}
	_, err = tx.Exec(ctx, `UPDATE `+s.table("jobs")+` SET status='cancelled',cancel_requested=true,updated_at=clock_timestamp(),finished_at=clock_timestamp() WHERE id=$1`, job)
	if err != nil {
		return empty, err
	}
	if err = s.event(ctx, tx, "job.cancelled", receipt); err != nil {
		return empty, err
	}
	return receipt, tx.Commit(ctx)
}

func (a *API) withdrawUnclaimed(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	var request struct {
		OperationID string `json:"operation_id"`
	}
	if !a.decode(w, r, &request) {
		return
	}
	receipt, err := a.Store.WithdrawUnclaimed(r.Context(), r.PathValue("id"), request.OperationID)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}
