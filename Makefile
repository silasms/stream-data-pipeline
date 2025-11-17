.PHONY: build run test test-bench clean

build:
	go build -o bin/pipeline ./cmd/pipeline

run: build
	./bin/pipeline

test:
	go test -v ./...

test-bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf bin/
