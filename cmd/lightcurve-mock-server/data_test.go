package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestLoadLightCurveDataset(
	t *testing.T,
) {
	t.Parallel()

	dataDir :=
		t.TempDir()

	writeMockLightCurveFile(
		t,
		filepath.Join(
			dataDir,
			"object-b.csv",
		),
		`MJD,Mag,Mag_err
59003,15.3,0.03
59001,15.1,0.01
59002,15.2,0.02
`,
	)

	writeMockLightCurveFile(
		t,
		filepath.Join(
			dataDir,
			"object-a.txt",
		),
		`# real-like whitespace data
time magnitude magnitude_error
58001 14.1 0.01
58002 14.2 0.02
58003 14.3 0.03
58004 14.4 0.04
`,
	)

	if err :=
		os.WriteFile(
			filepath.Join(
				dataDir,
				"README.md",
			),
			[]byte("ignored"),
			0o600,
		); err != nil {
		t.Fatalf(
			"WriteFile(README) error = %v",
			err,
		)
	}

	dataset, err :=
		loadLightCurveDataset(
			dataDir,
		)
	if err != nil {
		t.Fatalf(
			"loadLightCurveDataset() error = %v",
			err,
		)
	}

	wantObjectIDs :=
		[]string{
			"object-a",
			"object-b",
		}

	if !reflect.DeepEqual(
		dataset.ObjectIDs(),
		wantObjectIDs,
	) {
		t.Fatalf(
			"ObjectIDs() = %#v, want %#v",
			dataset.ObjectIDs(),
			wantObjectIDs,
		)
	}

	revision, ok :=
		dataset.Revision(
			"object-b",
			1,
		)
	if !ok {
		t.Fatal(
			"Revision(object-b, 1) found = false",
		)
	}

	if revision.ObjectID !=
		"object-b" {
		t.Fatalf(
			"ObjectID = %q, want %q",
			revision.ObjectID,
			"object-b",
		)
	}

	if revision.Revision != 1 {
		t.Fatalf(
			"Revision = %d, want 1",
			revision.Revision,
		)
	}

	if revision.EligibleEpochCount !=
		3 {
		t.Fatalf(
			"EligibleEpochCount = %d, want 3",
			revision.EligibleEpochCount,
		)
	}

	if revision.QualityPolicyVersion ==
		nil ||
		*revision.QualityPolicyVersion !=
			mockQualityPolicyVersion {
		t.Fatalf(
			"QualityPolicyVersion = %#v",
			revision.QualityPolicyVersion,
		)
	}

	gotTimes :=
		[]float64{
			revision.Epochs[0].
				ObservationTime,

			revision.Epochs[1].
				ObservationTime,

			revision.Epochs[2].
				ObservationTime,
		}

	wantTimes :=
		[]float64{
			59001,
			59002,
			59003,
		}

	if !reflect.DeepEqual(
		gotTimes,
		wantTimes,
	) {
		t.Fatalf(
			"observation times = %#v, want %#v",
			gotTimes,
			wantTimes,
		)
	}
}

func TestLightCurveDatasetRevisionReturnsCopy(
	t *testing.T,
) {
	t.Parallel()

	dataDir :=
		t.TempDir()

	writeMockLightCurveFile(
		t,
		filepath.Join(
			dataDir,
			"object-a.csv",
		),
		`time,mag,mag_err
1,10,0.1
2,11,0.1
3,12,0.1
`,
	)

	dataset, err :=
		loadLightCurveDataset(
			dataDir,
		)
	if err != nil {
		t.Fatalf(
			"loadLightCurveDataset() error = %v",
			err,
		)
	}

	first, ok :=
		dataset.Revision(
			"object-a",
			1,
		)
	if !ok {
		t.Fatal(
			"first Revision() found = false",
		)
	}

	first.Epochs[0].Magnitude =
		999

	*first.QualityPolicyVersion =
		"mutated"

	second, ok :=
		dataset.Revision(
			"object-a",
			1,
		)
	if !ok {
		t.Fatal(
			"second Revision() found = false",
		)
	}

	if second.Epochs[0].Magnitude ==
		999 {
		t.Fatal(
			"Revision() returned mutable shared epochs",
		)
	}

	if second.QualityPolicyVersion ==
		nil ||
		*second.QualityPolicyVersion !=
			mockQualityPolicyVersion {
		t.Fatal(
			"Revision() returned mutable shared quality policy",
		)
	}
}

func TestLightCurveDatasetRejectsUnsupportedRevision(
	t *testing.T,
) {
	t.Parallel()

	dataDir :=
		t.TempDir()

	writeMockLightCurveFile(
		t,
		filepath.Join(
			dataDir,
			"object-a.csv",
		),
		`1,10,0.1
2,11,0.1
3,12,0.1
`,
	)

	dataset, err :=
		loadLightCurveDataset(
			dataDir,
		)
	if err != nil {
		t.Fatalf(
			"loadLightCurveDataset() error = %v",
			err,
		)
	}

	if _, ok :=
		dataset.Revision(
			"object-a",
			2,
		); ok {
		t.Fatal(
			"Revision(object-a, 2) found = true",
		)
	}
}

func TestLoadLightCurveDatasetRejectsInvalidCurve(
	t *testing.T,
) {
	t.Parallel()

	dataDir :=
		t.TempDir()

	writeMockLightCurveFile(
		t,
		filepath.Join(
			dataDir,
			"invalid.csv",
		),
		`time,mag,mag_err
1,10,0.1
1,11,0.1
2,12,0.1
`,
	)

	_, err :=
		loadLightCurveDataset(
			dataDir,
		)

	if err == nil {
		t.Fatal(
			"loadLightCurveDataset() error = nil",
		)
	}

	if !strings.Contains(
		err.Error(),
		application.
			ErrDuplicateObservationTime.
			Error(),
	) {
		t.Fatalf(
			"error = %v, want duplicate observation time",
			err,
		)
	}
}

func TestLoadLightCurveDatasetRejectsEmptyDirectory(
	t *testing.T,
) {
	t.Parallel()

	if _, err :=
		loadLightCurveDataset(
			t.TempDir(),
		); err == nil {
		t.Fatal(
			"loadLightCurveDataset(empty) error = nil",
		)
	}
}

func TestParseLightCurveEpochsAcceptsHeaderAliases(
	t *testing.T,
) {
	t.Parallel()

	epochs, err :=
		parseLightCurveEpochs(
			strings.NewReader(
				`observation_time,magnitude,error
3,13,0.3
1,11,0.1
2,12,0.2
`,
			),
		)
	if err != nil {
		t.Fatalf(
			"parseLightCurveEpochs() error = %v",
			err,
		)
	}

	if len(epochs) != 3 {
		t.Fatalf(
			"len(epochs) = %d, want 3",
			len(epochs),
		)
	}
}

func writeMockLightCurveFile(
	t *testing.T,
	path string,
	content string,
) {
	t.Helper()

	if err :=
		os.WriteFile(
			path,
			[]byte(content),
			0o600,
		); err != nil {
		t.Fatalf(
			"WriteFile(%q) error = %v",
			path,
			err,
		)
	}
}
