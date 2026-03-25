.PHONY: gen-proto clean tidy build help stop

# Variables
PROTO_DIR = proto
PROTO_FILES = $(wildcard $(PROTO_DIR)/*.proto)

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

gen-proto: ## Generate Go code from Protobuf definitions
	@echo "Generating API code from Protobuf..."
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)
	@echo "Done."

tidy: ## Tidy Go module
	@echo "Tidying Go module..."
	go mod tidy

build: tidy ## Build the project
	@echo "Building project..."
	go build ./...

build-broker: tidy ## Build the broker binary
	@echo "Building broker..."
	go build -o bin/broker.exe ./cmd/broker

run-broker: build-broker ## Build and run the broker
	@echo "Starting broker..."
	./bin/broker.exe

build-producer: tidy ## Build the producer binary
	@echo "Building producer..."
	go build -o bin/producer.exe ./cmd/producer

run-producer: build-producer ## Build and run the producer
	@echo "Starting producer..."
	./bin/producer.exe

build-consumer: tidy ## Build the consumer binary
	@echo "Building consumer..."
	go build -o bin/consumer.exe ./cmd/consumer

run-consumer: build-consumer ## Build and run the consumer
	@echo "Starting consumer..."
	./bin/consumer.exe

stop: ## Close all relevant processes (broker, producer, consumer)
	@echo "Stopping broker and client processes..."
	-powershell -Command "Get-Process broker, producer, consumer -ErrorAction SilentlyContinue | Stop-Process -Force"
	@echo "Done."

test-integration: stop ## Run full integration test
	@echo "Running integration test..."
	powershell -ExecutionPolicy Bypass -File scripts/test_integration.ps1

test-mutiple-key: stop ## Run multiple key test
	@echo "Running multiple key test..."
	powershell -ExecutionPolicy Bypass -File scripts/test_keys.ps1
clean: ## Clean generated files
	@echo "Cleaning generated files..."
	powershell -Command "Remove-Item -Path $(PROTO_DIR)\*.pb.go -ErrorAction SilentlyContinue"
	powershell -Command "if (Test-Path bin) { Remove-Item -Path bin -Recurse -Force -ErrorAction SilentlyContinue }"
	@echo "Done."
