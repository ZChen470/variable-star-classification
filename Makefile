.PHONY: fmt fmt-check vet test build check

fmt:
	gofmt -w .

fmt-check:
	@files="$$(gofmt -l .)"; if [ -n "$$files" ]; then echo "$$files"; exit 1; fi

vet:
	go vet ./...

test:
	go test ./...

build:
	go build ./...

check: fmt-check vet test build