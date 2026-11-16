package gateway

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	ErrUnauthorized = errors.New("unauthorized")
	ErrNotFound     = errors.New("not_found")
	ErrConflict     = errors.New("conflict")
	ErrLeaseLost    = errors.New("lease_lost")
	ErrInvalid      = errors.New("invalid_request")
	ErrUnavailable  = errors.New("storage_unavailable")
)
var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$`)

func validID(id string) bool { return identifier.MatchString(id) }
func newID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic("secure random unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func NewToken() string {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		panic("secure random unavailable")
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
func tokenHash(s string) []byte { h := sha256.Sum256([]byte(s)); return h[:] }
func canonical(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage("null"), nil
	}
	var value any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if e := d.Decode(&value); e != nil {
		return nil, ErrInvalid
	}
	if e := d.Decode(new(any)); e != io.EOF {
		return nil, ErrInvalid
	}
	b, e := json.Marshal(value)
	return b, e
}
func digest(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		panic("invalid internal JSON")
	}
	sum := sha256.Sum256(b)
	return sum[:]
}
func sortedUnique(v []string) []string {
	m := map[string]bool{}
	for _, s := range v {
		if s != "" {
			m[s] = true
		}
	}
	a := make([]string, 0, len(m))
	for s := range m {
		a = append(a, s)
	}
	sort.Strings(a)
	return a
}
func shortHash(b []byte) string { return hex.EncodeToString(b) }

type WorkerHeartbeat struct {
	WorkerID     string   `json:"worker_id"`
	VersionCode  int64    `json:"version_code"`
	VersionName  string   `json:"version_name"`
	Capabilities []string `json:"capabilities"`
	ABIs         []string `json:"abis"`
	State        string   `json:"state"`
	ActiveJobID  string   `json:"active_job_id,omitempty"`
}
type EnqueueJob struct {
	IdempotencyKey       string          `json:"idempotency_key,omitempty"`
	Operation            string          `json:"operation"`
	Input                json.RawMessage `json:"input"`
	Network              json.RawMessage `json:"network,omitempty"`
	Script               string          `json:"script,omitempty"`
	RequiredCapabilities []string        `json:"required_capabilities,omitempty"`
	Priority             int             `json:"priority,omitempty"`
}

func (j EnqueueJob) normalized() (EnqueueJob, error) {
	if strings.TrimSpace(j.Operation) == "" {
		return j, ErrInvalid
	}
	if len(j.Input) == 0 {
		j.Input = json.RawMessage(`{}`)
	}
	var obj map[string]json.RawMessage
	if e := json.Unmarshal(j.Input, &obj); e != nil || obj == nil {
		return j, ErrInvalid
	}
	var e error
	j.Input, e = canonical(j.Input)
	if e != nil {
		return j, e
	}
	if len(j.Network) > 0 {
		if strings.TrimSpace(string(j.Network)) == "null" {
			j.Network = nil
		} else {
			var network map[string]json.RawMessage
			if json.Unmarshal(j.Network, &network) != nil || network == nil {
				return j, ErrInvalid
			}
			j.Network, e = canonical(j.Network)
			if e != nil {
				return j, e
			}
			if len(network) == 0 {
				j.Network = nil
			}
		}
	}
	if len(j.RequiredCapabilities) == 0 {
		j.RequiredCapabilities = []string{j.Operation}
	}
	j.RequiredCapabilities = sortedUnique(j.RequiredCapabilities)
	return j, nil
}

type ClaimedJob struct {
	ID           string          `json:"id"`
	Operation    string          `json:"operation"`
	Input        json.RawMessage `json:"input"`
	Network      json.RawMessage `json:"network,omitempty"`
	Script       string          `json:"script,omitempty"`
	AttemptID    string          `json:"attempt_id"`
	FencingToken int64           `json:"fencing_token"`
	LeaseUntil   time.Time       `json:"lease_until"`
}
type JobHeartbeat struct {
	WorkerID     string          `json:"worker_id"`
	AttemptID    string          `json:"attempt_id"`
	FencingToken int64           `json:"fencing_token"`
	Progress     json.RawMessage `json:"progress"`
}
type LeaseReceipt struct {
	LeaseUntil      time.Time `json:"lease_until"`
	CancelRequested bool      `json:"cancel_requested"`
}
type Complete struct {
	WorkerID     string          `json:"worker_id"`
	AttemptID    string          `json:"attempt_id"`
	FencingToken int64           `json:"fencing_token"`
	Status       string          `json:"status"`
	Result       json.RawMessage `json:"result"`
	Error        json.RawMessage `json:"error,omitempty"`
	ArtifactIDs  []string        `json:"artifact_ids,omitempty"`
}

func (c Complete) normalized() (Complete, error) {
	if c.Status != "succeeded" && c.Status != "failed" {
		return c, ErrInvalid
	}
	var e error
	c.ArtifactIDs = sortedUnique(c.ArtifactIDs)
	for _, id := range c.ArtifactIDs {
		if !uuidPattern.MatchString(id) {
			return c, ErrInvalid
		}
	}
	c.Result, e = canonical(c.Result)
	if e != nil {
		return c, e
	}
	c.Error, e = canonical(c.Error)
	return c, e
}

type CompletionReceipt struct {
	JobID          string `json:"job_id"`
	AttemptID      string `json:"attempt_id"`
	Committed      bool   `json:"committed"`
	CandidateSaved bool   `json:"candidate_saved,omitempty"`
}
type Command struct {
	EventID string     `json:"event_id"`
	Type    string     `json:"type"`
	Payload EnqueueJob `json:"payload"`
}
type CommandReceipt struct {
	EventID  string `json:"event_id"`
	Accepted bool   `json:"accepted"`
	JobID    string `json:"job_id,omitempty"`
	Error    string `json:"error,omitempty"`
}
