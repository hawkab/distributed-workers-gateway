package gateway

import (
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

var artifactKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$`)

type AttemptIdentity struct {
	WorkerID     string `json:"worker_id"`
	AttemptID    string `json:"attempt_id"`
	FencingToken int64  `json:"fencing_token"`
}

func (i AttemptIdentity) valid() bool {
	return validID(i.WorkerID) && uuidPattern.MatchString(i.AttemptID) && i.FencingToken > 0
}
func validSHA(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && strings.ToLower(s) == s
}

type CreateArtifact struct {
	AttemptIdentity
	Key       string `json:"key"`
	Name      string `json:"name,omitempty"`
	MediaType string `json:"media_type"`
}
type ArtifactRef struct {
	ArtifactID string `json:"artifact_id"`
	Name       string `json:"name"`
	SHA256     string `json:"sha256"`
	Length     int64  `json:"length"`
	MediaType  string `json:"media_type"`
}
type ArtifactReceipt struct {
	ArtifactRef
	Committed      bool `json:"committed"`
	CandidateSaved bool `json:"candidate_saved"`
}
type ArtifactChunk struct {
	UploadID   string `json:"upload_id,omitempty"`
	Offset     int64  `json:"offset"`
	NextOffset int64  `json:"next_offset"`
	Length     int64  `json:"length"`
	SHA256     string `json:"sha256"`
}
type ArtifactStatus struct {
	UploadID        string           `json:"upload_id"`
	Status          string           `json:"status"`
	Offset          int64            `json:"offset"`
	LastSegment     *ArtifactChunk   `json:"last_segment,omitempty"`
	SegmentsCount   int64            `json:"segments_count"`
	LeaseUntil      time.Time        `json:"lease_until"`
	CancelRequested bool             `json:"cancel_requested"`
	Artifact        *ArtifactReceipt `json:"artifact,omitempty"`
}
type FinishArtifact struct {
	AttemptIdentity
	Length int64  `json:"length"`
	SHA256 string `json:"sha256,omitempty"`
}
type EffectBegin struct {
	AttemptIdentity
	ExecutionID string          `json:"execution_id"`
	Request     json.RawMessage `json:"request"`
}
type EffectFinish struct {
	AttemptIdentity
	ExecutionID   string          `json:"execution_id"`
	RequestSHA256 string          `json:"request_sha256"`
	Response      json.RawMessage `json:"response"`
	ArtifactIDs   []string        `json:"artifact_ids,omitempty"`
}
type EffectReceipt struct {
	EffectID       string          `json:"effect_id"`
	State          string          `json:"state,omitempty"`
	ExecutionID    string          `json:"execution_id"`
	RequestSHA256  string          `json:"request_sha256,omitempty"`
	Response       json.RawMessage `json:"response,omitempty"`
	ArtifactIDs    []string        `json:"artifact_ids,omitempty"`
	Committed      bool            `json:"committed,omitempty"`
	CandidateSaved bool            `json:"candidate_saved,omitempty"`
	Recorded       bool            `json:"recorded,omitempty"`
}
