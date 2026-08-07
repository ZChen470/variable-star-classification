package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

const maxUploadBytes int64 = 1 << 20 // 1 MiB

var errInvalidUploadedLightCurve = errors.New(
	"invalid uploaded light curve",
)

func parseUploadedLightCurve(
	reader io.Reader,
) ([]domain.LightCurveEpoch, error) {
	if reader == nil {
		return nil, fmt.Errorf(
			"%w: reader is nil",
			errInvalidUploadedLightCurve,
		)
	}

	scanner := bufio.NewScanner(
		io.LimitReader(
			reader,
			maxUploadBytes+1,
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
	seenDataOrHeader := false

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
			splitUploadedLine(line)

		if len(fields) != 3 {
			return nil, fmt.Errorf(
				"%w: line %d has %d columns, want exactly 3",
				errInvalidUploadedLightCurve,
				lineNumber,
				len(fields),
			)
		}

		if !seenDataOrHeader &&
			isAcceptedHeader(fields) {
			seenDataOrHeader = true
			continue
		}

		seenDataOrHeader = true

		timeValue, err :=
			strconv.ParseFloat(
				fields[0],
				64,
			)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: line %d invalid time %q: %v",
				errInvalidUploadedLightCurve,
				lineNumber,
				fields[0],
				err,
			)
		}

		magnitudeValue, err :=
			strconv.ParseFloat(
				fields[1],
				32,
			)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: line %d invalid magnitude %q: %v",
				errInvalidUploadedLightCurve,
				lineNumber,
				fields[1],
				err,
			)
		}

		magnitudeErrorValue, err :=
			strconv.ParseFloat(
				fields[2],
				32,
			)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: line %d invalid magnitude_error %q: %v",
				errInvalidUploadedLightCurve,
				lineNumber,
				fields[2],
				err,
			)
		}

		epochs = append(
			epochs,
			domain.LightCurveEpoch{
				ObservationTime: timeValue,

				Magnitude: float32(
					magnitudeValue,
				),

				MagnitudeError: float32(
					magnitudeErrorValue,
				),
			},
		)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf(
			"%w: read upload: %v",
			errInvalidUploadedLightCurve,
			err,
		)
	}

	if len(epochs) == 0 {
		return nil, fmt.Errorf(
			"%w: file contains no data rows",
			errInvalidUploadedLightCurve,
		)
	}

	return epochs, nil
}

func splitUploadedLine(
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

func isAcceptedHeader(
	fields []string,
) bool {
	if len(fields) != 3 {
		return false
	}

	return isOneOf(
		normalizeHeader(fields[0]),
		"time",
		"time_mjd",
		"mjd",
		"observation_time",
	) &&
		isOneOf(
			normalizeHeader(fields[1]),
			"magnitude",
			"mag",
		) &&
		isOneOf(
			normalizeHeader(fields[2]),
			"magnitude_error",
			"mag_err",
			"magerror",
			"error",
		)
}

func normalizeHeader(
	value string,
) string {
	return strings.ToLower(
		strings.TrimSpace(value),
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

func buildScienceClassificationInput(
	epochs []domain.LightCurveEpoch,
) (
	application.ClassificationInput,
	application.CoarseMode,
	error,
) {
	declaredCount :=
		uint32(len(epochs))

	revision :=
		domain.LightCurveRevision{
			ObjectID: "science-upload",

			Revision: 1,

			EligibleEpochCount: declaredCount,

			Epochs: append(
				[]domain.LightCurveEpoch(nil),
				epochs...,
			),
		}

	prepared, err :=
		application.
			PrepareLightCurveRevision(
				revision,
				declaredCount,
			)
	if err != nil {
		return application.ClassificationInput{},
			application.CoarseModeUnspecified,
			err
	}

	mode :=
		application.
			CoarseModeComputeCurrent

	if len(prepared.Epochs) > 20 {
		// 这个科学测试入口没有 PostgreSQL，
		// 因而不存在可以复用的历史粗概率。
		//
		// n > 20 时直接使用 bootstrap，
		// 让 Triton 在本次请求中执行 XGBoost。
		mode =
			application.
				CoarseModeComputeBootstrap
	}

	input, err :=
		application.
			BuildClassificationInput(
				prepared,
				application.
					CoarseModeSelection{
					Mode: mode,
				},
			)
	if err != nil {
		return application.ClassificationInput{},
			application.CoarseModeUnspecified,
			err
	}

	return input, mode, nil
}
