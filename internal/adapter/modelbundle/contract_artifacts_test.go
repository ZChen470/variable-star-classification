package modelbundle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRepositoryContractArtifactHashes(t *testing.T) {
	manifestContent, err := os.ReadFile(repositoryManifestPath(t))
	if err != nil {
		t.Fatalf("os.ReadFile(manifest) error = %v", err)
	}

	var manifest repositoryContractArtifactManifest
	if err := yaml.Unmarshal(manifestContent, &manifest); err != nil {
		t.Fatalf("yaml.Unmarshal(manifest) error = %v", err)
	}

	bundleDirectory := filepath.Dir(repositoryManifestPath(t))
	artifacts := manifest.ContractArtifacts

	verifyRepositoryArtifactHash(
		t,
		bundleDirectory,
		"serving contract",
		artifacts.ServingContract,
	)
	verifyRepositoryArtifactHash(
		t,
		bundleDirectory,
		"model metadata fixture",
		artifacts.ModelMetadataFixture,
	)
	verifyRepositoryArtifactHash(
		t,
		bundleDirectory,
		"model config fixture",
		artifacts.ModelConfigFixture,
	)

	wantModes := []string{
		"compute_current",
		"compute_bootstrap",
		"reuse_previous",
	}
	if len(artifacts.RequestResponseFixtures) != len(wantModes) {
		t.Fatalf(
			"request_response_fixtures count = %d, want %d",
			len(artifacts.RequestResponseFixtures),
			len(wantModes),
		)
	}

	for _, mode := range wantModes {
		fixture, ok := artifacts.RequestResponseFixtures[mode]
		if !ok {
			t.Fatalf("request_response_fixtures.%s is missing", mode)
		}

		verifyRepositoryArtifactHash(
			t,
			bundleDirectory,
			mode+" request",
			repositoryArtifactReference{
				File:   fixture.RequestFile,
				SHA256: fixture.RequestSHA256,
			},
		)
		verifyRepositoryArtifactHash(
			t,
			bundleDirectory,
			mode+" response",
			repositoryArtifactReference{
				File:   fixture.ResponseFile,
				SHA256: fixture.ResponseSHA256,
			},
		)
	}
}

func verifyRepositoryArtifactHash(
	t *testing.T,
	bundleDirectory string,
	name string,
	reference repositoryArtifactReference,
) {
	t.Helper()

	if reference.File == "" {
		t.Fatalf("%s file is empty", name)
	}
	if reference.SHA256 == "" {
		t.Fatalf("%s SHA-256 is empty", name)
	}
	if reference.SHA256 != strings.ToLower(reference.SHA256) {
		t.Fatalf("%s SHA-256 must use lowercase hexadecimal", name)
	}
	decodedHash, err := hex.DecodeString(reference.SHA256)
	if err != nil || len(decodedHash) != sha256.Size {
		t.Fatalf("%s SHA-256 %q is invalid", name, reference.SHA256)
	}

	cleanPath := filepath.Clean(reference.File)
	if filepath.IsAbs(cleanPath) ||
		cleanPath == ".." ||
		strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) {
		t.Fatalf("%s file %q escapes the bundle directory", name, reference.File)
	}

	content, err := os.ReadFile(filepath.Join(bundleDirectory, cleanPath))
	if err != nil {
		t.Fatalf("os.ReadFile(%s) error = %v", name, err)
	}
	actualHash := fmt.Sprintf("%x", sha256.Sum256(content))
	if actualHash != reference.SHA256 {
		t.Fatalf(
			"%s SHA-256 = %s, want %s",
			name,
			actualHash,
			reference.SHA256,
		)
	}
}

type repositoryContractArtifactManifest struct {
	ContractArtifacts repositoryContractArtifacts `yaml:"contract_artifacts"`
}

type repositoryContractArtifacts struct {
	ServingContract         repositoryArtifactReference                 `yaml:"serving_contract"`
	ModelMetadataFixture    repositoryArtifactReference                 `yaml:"model_metadata_fixture"`
	ModelConfigFixture      repositoryArtifactReference                 `yaml:"model_config_fixture"`
	RequestResponseFixtures map[string]repositoryRequestResponseFixture `yaml:"request_response_fixtures"`
}

type repositoryArtifactReference struct {
	File   string `yaml:"file"`
	SHA256 string `yaml:"sha256"`
}

type repositoryRequestResponseFixture struct {
	RequestFile    string `yaml:"request_file"`
	RequestSHA256  string `yaml:"request_sha256"`
	ResponseFile   string `yaml:"response_file"`
	ResponseSHA256 string `yaml:"response_sha256"`
}
