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

proto-check: proto-format-check proto-lint proto-build

vet:
	go vet ./...

test:
	go test ./...

build:
	go build ./...

check: fmt-check proto-check vet test build