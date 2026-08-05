package domain

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"math"
	"strings"
)

// ExecutionMode 表示分类任务的执行用途
type ExecutionMode uint8

const (
	ExecutionModeUnspecified ExecutionMode = iota
	ExecutionModeProduction
	ExecutionModeShadow
	ExecutionModeReprocess
)

// JobIdentity 包含决定逻辑任务唯一身份的全部字段
//
// candidate_revision、priority、时间和 trace 不参与任务身份。
type JobIdentity struct {
	ObjectID           string
	LightCurveRevision int64
	ModelBundleVersion string
	ExecutionMode      ExecutionMode
}

type JobID string

type RunID string

const (
	jobNamePrefix = "astro.classification.job.v1"
	runName       = "classification-run-v1"
)

// GenerateJobID 根据冻结的 v1 编码规则生成确定性 JobID
func GenerateJobID(identity JobIdentity) (JobID, error) {
	if err := validateJobIdentity(identity); err != nil {
		return "", err
	}

	payload := make([]byte, 0, 128)
	payload = appendLengthPrefixedString(payload, jobNamePrefix)
	payload = appendLengthPrefixedString(payload, identity.ObjectID)

	var revision [8]byte
	binary.BigEndian.PutUint64(revision[:], uint64(identity.LightCurveRevision))
	payload = append(payload, revision[:]...)

	payload = appendLengthPrefixedString(payload, identity.ModelBundleVersion)
	//payload = appendLengthPrefixedString(payload, identity.ClassificationPolicyVersion)
	payload = append(payload, byte(identity.ExecutionMode))

	id := uuid.NewSHA1(uuid.NameSpaceURL, payload)
	return JobID(id.String()), nil
}

// GenerateRunID 根据 JobID 生成确定性的成功 RunID
func GenerateRunID(jobID JobID) (RunID, error) {
	parsed, err := uuid.Parse(string(jobID))
	if err != nil {
		return "", fmt.Errorf("parse job ID: %w", err)
	}
	if parsed == uuid.Nil {
		return "", fmt.Errorf("job ID must not be nil UUID")
	}

	id := uuid.NewSHA1(parsed, []byte(runName))
	return RunID(id.String()), nil
}

// validateJobIdentity 验证 JobIdentity
func validateJobIdentity(identity JobIdentity) error {
	if err := validateIdentityString("object ID", identity.ObjectID); err != nil {
		return err
	}
	if identity.LightCurveRevision <= 0 {
		return fmt.Errorf("light curve revision must be greater than zero")
	}
	if err := validateIdentityString("model bundle version", identity.ModelBundleVersion); err != nil {
		return err
	}
	//if err := validateIdentityString("classification policy version", identity.ClassificationPolicyVersion); err != nil {return err}

	switch identity.ExecutionMode {
	case ExecutionModeProduction, ExecutionModeShadow, ExecutionModeReprocess:
		return nil
	default:
		return fmt.Errorf("invalid execution mode: %d", identity.ExecutionMode)
	}
}

// validateIdentityString 验证字符串
func validateIdentityString(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", field)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must not contain leading or trailing whitespace", field)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must not contain NUL", field)
	}
	if uint64(len(value)) > math.MaxUint32 {
		return fmt.Errorf("%s is too long", field)
	}
	return nil
}

func appendLengthPrefixedString(dst []byte, value string) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))

	dst = append(dst, length[:]...)
	return append(dst, value...)
}
