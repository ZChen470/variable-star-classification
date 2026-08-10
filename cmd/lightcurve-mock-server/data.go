package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

const (
	mockLightCurveRevision int64 = 1

	mockQualityPolicyVersion = "mock-v1"

	maxMockLightCurveFileBytes int64 = 1 << 20
)

type lightCurveDataset struct {
	objectIDs []string

	revisions map[string]domain.LightCurveRevision
}

func loadLightCurveDataset(
	dataDir string,
) (lightCurveDataset, error) {
	if strings.TrimSpace(dataDir) == "" {
		return lightCurveDataset{},
			errors.New(
				"light curve mock data directory must not be blank",
			)
	}

	entries, err :=
		os.ReadDir(dataDir)
	if err != nil {
		return lightCurveDataset{},
			fmt.Errorf(
				"read light curve mock data directory %q: %w",
				dataDir,
				err,
			)
	}

	dataset := lightCurveDataset{
		objectIDs: make(
			[]string,
			0,
			len(entries),
		),

		revisions: make(
			map[string]domain.LightCurveRevision,
		),
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		extension :=
			strings.ToLower(
				filepath.Ext(
					entry.Name(),
				),
			)

		if extension != ".csv" &&
			extension != ".txt" {
			continue
		}

		objectID :=
			strings.TrimSuffix(
				entry.Name(),
				filepath.Ext(
					entry.Name(),
				),
			)

		if err :=
			validateMockObjectID(
				objectID,
			); err != nil {
			return lightCurveDataset{},
				fmt.Errorf(
					"invalid object id derived from %q: %w",
					entry.Name(),
					err,
				)
		}

		if _, exists := dataset.revisions[objectID]; exists {
			return lightCurveDataset{},
				fmt.Errorf(
					"duplicate light curve mock object_id %q",
					objectID,
				)
		}

		path :=
			filepath.Join(
				dataDir,
				entry.Name(),
			)

		revision, err :=
			loadLightCurveRevisionFile(
				path,
				objectID,
			)
		if err != nil {
			return lightCurveDataset{},
				err
		}

		dataset.objectIDs =
			append(
				dataset.objectIDs,
				objectID,
			)

		dataset.revisions[objectID] = revision
	}

	if len(dataset.objectIDs) == 0 {
		return lightCurveDataset{},
			fmt.Errorf(
				"light curve mock data directory %q contains no .csv or .txt files",
				dataDir,
			)
	}

	return dataset, nil
}

func (
	dataset lightCurveDataset,
) ObjectIDs() []string {
	return append(
		[]string(nil),
		dataset.objectIDs...,
	)
}

func (
	dataset lightCurveDataset,
) Revision(
	objectID string,
	revision int64,
) (domain.LightCurveRevision, bool) {
	if revision !=
		mockLightCurveRevision {
		return domain.LightCurveRevision{},
			false
	}

	value, ok :=
		dataset.revisions[objectID]
	if !ok {
		return domain.LightCurveRevision{},
			false
	}

	return cloneLightCurveRevision(
		value,
	), true
}

func loadLightCurveRevisionFile(
	path string,
	objectID string,
) (domain.LightCurveRevision, error) {
	file, err :=
		os.Open(path)
	if err != nil {
		return domain.LightCurveRevision{},
			fmt.Errorf(
				"open light curve mock file %q: %w",
				path,
				err,
			)
	}
	defer file.Close()

	epochs, err :=
		parseLightCurveEpochs(file)
	if err != nil {
		return domain.LightCurveRevision{},
			fmt.Errorf(
				"parse light curve mock file %q: %w",
				path,
				err,
			)
	}

	eligibleEpochCount :=
		uint32(len(epochs))

	qualityPolicyVersion :=
		mockQualityPolicyVersion

	revision :=
		domain.LightCurveRevision{
			ObjectID: objectID,

			Revision: mockLightCurveRevision,

			EligibleEpochCount: eligibleEpochCount,

			QualityPolicyVersion: &qualityPolicyVersion,

			Epochs: epochs,
		}

	prepared, err :=
		application.
			PrepareLightCurveRevision(
				revision,
				eligibleEpochCount,
			)
	if err != nil {
		return domain.LightCurveRevision{},
			fmt.Errorf(
				"prepare light curve mock object_id=%q revision=%d: %w",
				objectID,
				mockLightCurveRevision,
				err,
			)
	}

	return prepared, nil
}

func parseLightCurveEpochs(
	reader io.Reader,
) ([]domain.LightCurveEpoch, error) {
	if reader == nil {
		return nil,
			errors.New(
				"light curve reader must not be nil",
			)
	}

	scanner :=
		bufio.NewScanner(
			io.LimitReader(
				reader,
				maxMockLightCurveFileBytes+1,
			),
		)

	scanner.Buffer(
		make([]byte, 4096),
		256*1024,
	)

	epochs := make(
		[]domain.LightCurveEpoch,
		0,
		64,
	)

	lineNumber := 0

	seenDataOrHeader :=
		false

	for scanner.Scan() {
		lineNumber++

		line :=
			strings.TrimSpace(
				scanner.Text(),
			)

		if line == "" ||
			strings.HasPrefix(
				line,
				"#",
			) {
			continue
		}

		fields :=
			splitLightCurveLine(
				line,
			)

		if len(fields) != 3 {
			return nil,
				fmt.Errorf(
					"line %d has %d columns, want exactly 3",
					lineNumber,
					len(fields),
				)
		}

		if !seenDataOrHeader &&
			isAcceptedLightCurveHeader(
				fields,
			) {
			seenDataOrHeader = true
			continue
		}

		seenDataOrHeader = true

		observationTime, err :=
			strconv.ParseFloat(
				fields[0],
				64,
			)
		if err != nil {
			return nil,
				fmt.Errorf(
					"line %d invalid observation_time %q: %w",
					lineNumber,
					fields[0],
					err,
				)
		}

		magnitude, err :=
			strconv.ParseFloat(
				fields[1],
				32,
			)
		if err != nil {
			return nil,
				fmt.Errorf(
					"line %d invalid magnitude %q: %w",
					lineNumber,
					fields[1],
					err,
				)
		}

		magnitudeError, err :=
			strconv.ParseFloat(
				fields[2],
				32,
			)
		if err != nil {
			return nil,
				fmt.Errorf(
					"line %d invalid magnitude_error %q: %w",
					lineNumber,
					fields[2],
					err,
				)
		}

		epochs = append(
			epochs,
			domain.LightCurveEpoch{
				ObservationTime: observationTime,

				Magnitude: float32(
					magnitude,
				),

				MagnitudeError: float32(
					magnitudeError,
				),
			},
		)
	}

	if err := scanner.Err(); err != nil {
		return nil,
			fmt.Errorf(
				"read light curve data: %w",
				err,
			)
	}

	if len(epochs) == 0 {
		return nil,
			errors.New(
				"light curve file contains no data rows",
			)
	}

	return epochs, nil
}

func splitLightCurveLine(
	line string,
) []string {
	if strings.Contains(
		line,
		",",
	) {
		parts :=
			strings.Split(
				line,
				",",
			)

		for index := range parts {
			parts[index] =
				strings.TrimSpace(
					parts[index],
				)
		}

		return parts
	}

	return strings.Fields(line)
}

func isAcceptedLightCurveHeader(
	fields []string,
) bool {
	if len(fields) != 3 {
		return false
	}

	return isOneOf(
		normalizeLightCurveHeader(
			fields[0],
		),
		"time",
		"time_mjd",
		"mjd",
		"observation_time",
	) &&
		isOneOf(
			normalizeLightCurveHeader(
				fields[1],
			),
			"magnitude",
			"mag",
		) &&
		isOneOf(
			normalizeLightCurveHeader(
				fields[2],
			),
			"magnitude_error",
			"mag_err",
			"magerror",
			"error",
		)
}

func normalizeLightCurveHeader(
	value string,
) string {
	return strings.ToLower(
		strings.TrimSpace(
			value,
		),
	)
}

func isOneOf(
	value string,
	choices ...string,
) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}

	return false
}

func validateMockObjectID(
	value string,
) error {
	if value == "" {
		return errors.New(
			"must not be empty",
		)
	}

	if strings.TrimSpace(value) !=
		value {
		return errors.New(
			"must not contain leading or trailing whitespace",
		)
	}

	if strings.ContainsRune(
		value,
		'\x00',
	) {
		return errors.New(
			"must not contain NUL",
		)
	}

	return nil
}

func cloneLightCurveRevision(
	revision domain.LightCurveRevision,
) domain.LightCurveRevision {
	cloned := revision

	cloned.Epochs = append(
		[]domain.LightCurveEpoch(nil),
		revision.Epochs...,
	)

	if revision.QualityPolicyVersion != nil {
		value := *revision.QualityPolicyVersion
		cloned.QualityPolicyVersion = &value
	}

	return cloned
}
