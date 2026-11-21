VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
TARGETS := linux/arm64 linux/arm/7 linux/amd64

.PHONY: all build test race vet cross demo clean

all: vet test build

build:
	go build -ldflags="$(LDFLAGS)" -o bin/ ./cmd/...

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

# Static, cgo-free binaries for embedded Linux targets.
cross:
	@for t in $(TARGETS); do \
		os=$$(echo $$t | cut -d/ -f1); arch=$$(echo $$t | cut -d/ -f2); arm=$$(echo $$t | cut -d/ -f3); \
		out=dist/sitebridge-$$os-$$arch$${arm:+v$$arm}; \
		echo "building $$out"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch GOARM=$$arm \
			go build -trimpath -ldflags="$(LDFLAGS)" -o $$out ./cmd/sitebridge || exit 1; \
	done

demo: build
	./scripts/demo.sh

clean:
	rm -rf bin dist data
