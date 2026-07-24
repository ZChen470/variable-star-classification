.PHONY: fmt fmt-check proto-format proto-format-check proto-lint proto-build proto-generate proto-check vet test build check

fmt:
	gofmt -w .

fmt-check:
	@files="$$(gofmt -l .)"; if [ -n "$$files" ]; then echo "$$files"; exit 1; fi

proto-format:
	buf format -w

proto-format-check:
	buf format --exit-code

proto-lint:
	buf lint

proto-build:
	buf build

proto-generate:
	buf generate
	@test -z "$$(git status --porcelain -- gen/go)" || { \
		echo "generated Protobuf Go code is not up to date"; \
		git status --short -- gen/go; \
		git diff -- gen/go; \
		exit 1; \
	}

proto-check: proto-format-check proto-lint proto-build

mod-tidy-check:
	go mod tidy
	@test -z "$$(git status --porcelain -- go.mod go.sum)" || { \
		echo "go.mod or go.sum is not tidy"; \
		git status --short -- go.mod go.sum; \
		git diff -- go.mod go.sum; \
		exit 1; \
	}

vet:
	go vet ./...

test:
	go test ./...

build:
	go build ./...

check: fmt-check proto-check vet test build

ci:
	$(MAKE) fmt-check
	$(MAKE) proto-check
	$(MAKE) proto-generate-check
	$(MAKE) mod-tidy-check
	$(MAKE) vet
	$(MAKE) test
	$(MAKE) build