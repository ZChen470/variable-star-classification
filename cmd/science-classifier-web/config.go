package main

import (
	"errors"
	"fmt"
	"strings"
)

const (
	envScienceWebListenAddr    = "SCIENCE_WEB_LISTEN_ADDR"
	envTritonBaseURL           = "TRITON_BASE_URL"
	envModelBundleVersion      = "MODEL_BUNDLE_VERSION"
	envModelBundleManifestPath = "MODEL_BUNDLE_MANIFEST_PATH"

	defaultScienceWebListenAddr = "127.0.0.1:8088"
)

type config struct {
	listenAddr              string
	tritonBaseURL           string
	modelBundleVersion      string
	modelBundleManifestPath string
}

func loadConfig(
	lookup func(string) (string, bool),
) (config, error) {
	if lookup == nil {
		return config{},
			errors.New(
				"environment lookup must not be nil",
			)
	}

	tritonBaseURL, err :=
		requiredExactEnv(
			lookup,
			envTritonBaseURL,
		)
	if err != nil {
		return config{}, err
	}

	modelBundleVersion, err :=
		requiredExactEnv(
			lookup,
			envModelBundleVersion,
		)
	if err != nil {
		return config{}, err
	}

	manifestPath, err :=
		requiredExactEnv(
			lookup,
			envModelBundleManifestPath,
		)
	if err != nil {
		return config{}, err
	}

	listenAddr :=
		defaultScienceWebListenAddr

	if value, ok :=
		lookup(envScienceWebListenAddr); ok {
		trimmed :=
			strings.TrimSpace(value)

		if trimmed != "" {
			listenAddr = trimmed
		}
	}

	return config{
		listenAddr: listenAddr,

		tritonBaseURL: tritonBaseURL,

		modelBundleVersion: modelBundleVersion,

		modelBundleManifestPath: manifestPath,
	}, nil
}

func requiredExactEnv(
	lookup func(string) (string, bool),
	name string,
) (string, error) {
	value, ok := lookup(name)

	if !ok ||
		strings.TrimSpace(value) == "" {
		return "", fmt.Errorf(
			"environment variable %s must be set and non-blank",
			name,
		)
	}

	if strings.TrimSpace(value) != value {
		return "", fmt.Errorf(
			"environment variable %s must not contain surrounding whitespace",
			name,
		)
	}

	return value, nil
}
