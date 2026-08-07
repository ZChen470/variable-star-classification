package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestParseUploadedLightCurveCSVWithHeader(t *testing.T) {
	input := `time,magnitude,magnitude_error
60003,14.30,0.03
60001,14.10,0.01
60002,14.20,0.02
`

	epochs, err := parseUploadedLightCurve(
		strings.NewReader(input),
	)
	if err != nil {
		t.Fatalf(
			"parseUploadedLightCurve() error = %v",
			err,
		)
	}

	if len(epochs) != 3 {
		t.Fatalf(
			"epoch count = %d, want 3",
			len(epochs),
		)
	}

	// Parser 本身不排序。
	if epochs[0].ObservationTime != 60003 {
		t.Fatalf(
			"first observation time = %v, want 60003",
			epochs[0].ObservationTime,
		)
	}
}

func TestParseUploadedLightCurveWhitespaceWithoutHeader(t *testing.T) {
	input := `60001 14.10 0.01
60002 14.20 0.02
60003 14.30 0.03
`

	epochs, err := parseUploadedLightCurve(
		strings.NewReader(input),
	)
	if err != nil {
		t.Fatalf(
			"parseUploadedLightCurve() error = %v",
			err,
		)
	}

	if len(epochs) != 3 {
		t.Fatalf(
			"epoch count = %d, want 3",
			len(epochs),
		)
	}
}

func TestBuildScienceClassificationInputCoarseModeBoundary(
	t *testing.T,
) {
	tests := []struct {
		name string
		n    int
		want application.CoarseMode
	}{
		{
			name: "20 epochs compute current",
			n:    20,
			want: application.CoarseModeComputeCurrent,
		},
		{
			name: "21 epochs bootstrap",
			n:    21,
			want: application.CoarseModeComputeBootstrap,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var builder strings.Builder

			for index := 0; index < test.n; index++ {
				_, _ = builder.WriteString(
					formatEpochLine(index),
				)
			}

			epochs, err := parseUploadedLightCurve(
				strings.NewReader(
					builder.String(),
				),
			)
			if err != nil {
				t.Fatalf(
					"parseUploadedLightCurve() error = %v",
					err,
				)
			}

			input, mode, err :=
				buildScienceClassificationInput(
					epochs,
				)
			if err != nil {
				t.Fatalf(
					"buildScienceClassificationInput() error = %v",
					err,
				)
			}

			if mode != test.want {
				t.Fatalf(
					"mode = %v, want %v",
					mode,
					test.want,
				)
			}

			if len(input.TimeMJD) != test.n {
				t.Fatalf(
					"input epoch count = %d, want %d",
					len(input.TimeMJD),
					test.n,
				)
			}

			if input.ReusedCoarseProbabilities != nil {
				t.Fatal(
					"ReusedCoarseProbabilities != nil, want nil",
				)
			}
		})
	}
}

func TestBuildScienceClassificationInputSortsByTime(
	t *testing.T,
) {
	epochs, err := parseUploadedLightCurve(
		strings.NewReader(
			`60003 14.3 0.03
60001 14.1 0.01
60002 14.2 0.02
`,
		),
	)
	if err != nil {
		t.Fatalf(
			"parseUploadedLightCurve() error = %v",
			err,
		)
	}

	input, _, err :=
		buildScienceClassificationInput(
			epochs,
		)
	if err != nil {
		t.Fatalf(
			"buildScienceClassificationInput() error = %v",
			err,
		)
	}

	want := []float64{
		60001,
		60002,
		60003,
	}

	for index := range want {
		if input.TimeMJD[index] != want[index] {
			t.Fatalf(
				"TimeMJD[%d] = %v, want %v",
				index,
				input.TimeMJD[index],
				want[index],
			)
		}
	}
}

func formatEpochLine(
	index int,
) string {
	return fmt.Sprintf(
		"%d 14.%02d 0.02\n",
		60000+index,
		index,
	)
}
