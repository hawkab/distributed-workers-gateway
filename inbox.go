package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
)

func (s *Store) AcceptCommand(ctx context.Context, c Command) (CommandReceipt, error) {
	receipt := CommandReceipt{EventID: c.EventID}
	if !validID(c.EventID) {
		return receipt, ErrInvalid
	}
	j, valid := c.Payload.normalized()
	if valid == nil {
		c.Payload = j
	}
	raw, e := json.Marshal(c)
	if e != nil {
		return receipt, e
	}
	hash := digest(c)
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return receipt, e
	}
	defer tx.Rollback(ctx)
	// Serialize same event IDs across concurrent broker deliveries and HTTP calls.
	_, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.Schema+":inbox:"+c.EventID)
	if e != nil {
		return receipt, e
	}
	var priorHash, priorReceipt []byte
	e = tx.QueryRow(ctx, `SELECT content_hash,receipt FROM `+s.table("inbox")+` WHERE event_id=$1`, c.EventID).Scan(&priorHash, &priorReceipt)
	if e == nil {
		if sameBytes(priorHash, hash) {
			e = json.Unmarshal(priorReceipt, &receipt)
			if e != nil {
				return receipt, e
			}
			return receipt, tx.Commit(ctx)
		}
		_, e = tx.Exec(ctx, `INSERT INTO `+s.table("inbox_conflicts")+`(event_id,content_hash,payload) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, c.EventID, hash, raw)
		if e != nil {
			return receipt, e
		}
		receipt.Error = "event_id_conflict"
		return receipt, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return receipt, e
	}
	if c.Type != "job.enqueue" {
		receipt.Error = "unsupported_event_type"
	} else if valid != nil {
		receipt.Error = "invalid_job"
	} else {
		if j.IdempotencyKey == "" {
			j.IdempotencyKey = "inbox:" + c.EventID
		}
		receipt.JobID, e = s.enqueue(ctx, tx, j)
		if e != nil {
			return receipt, e
		}
		receipt.Accepted = true
	}
	status := "accepted"
	if !receipt.Accepted {
		status = "rejected"
	}
	ack, _ := json.Marshal(receipt)
	_, e = tx.Exec(ctx, `INSERT INTO `+s.table("inbox")+`(event_id,content_hash,payload,status,receipt) VALUES($1,$2,$3,$4,$5)`, c.EventID, hash, raw, status, ack)
	if e != nil {
		return receipt, e
	}
	return receipt, tx.Commit(ctx)
}

// Invalid broker bodies are retained before acknowledgement, never discarded.
func (s *Store) RejectCommand(ctx context.Context, body []byte, code string) error {
	raw, _ := json.Marshal(map[string]any{"body": body, "reason": code})
	id := "invalid:" + shortHash(digest(body))
	receipt, _ := json.Marshal(CommandReceipt{EventID: id, Error: code})
	_, e := s.Pool.Exec(ctx, `INSERT INTO `+s.table("inbox")+`(event_id,content_hash,payload,status,receipt) VALUES($1,$2,$3,'rejected',$4) ON CONFLICT DO NOTHING`, id, digest(body), raw, receipt)
	return e
}
