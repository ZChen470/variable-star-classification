package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	config, err :=
		loadLightCurveMockConfig(
			os.LookupEnv,
		)
	if err != nil {
		log.Fatal(
			fmt.Errorf(
				"load light curve mock config: %w",
				err,
			),
		)
	}

	dataset, err :=
		loadLightCurveDataset(
			config.dataDir,
		)
	if err != nil {
		log.Fatal(
			fmt.Errorf(
				"load light curve mock dataset: %w",
				err,
			),
		)
	}

	log.Printf(
		"loaded %d light curve mock objects from %s",
		len(dataset.ObjectIDs()),
		config.dataDir,
	)
}
