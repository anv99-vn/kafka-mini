# Kafka-Mini

Kafka-Mini is a lightweight, distributed message broker written in Go, inspired by Apache Kafka. It implements core messaging concepts including topics, partitions, consumer groups, offset management, and high availability through the Raft consensus algorithm.

## Features

- **Topics & Partitions**: Messages are organized into topics and divided into partitions for horizontal scalability.
- **Consumer Groups**: Consumers can form groups to share the workload of reading from a topic. Dynamic rebalancing is supported (Round-Robin assignment).
- **Persistent Storage**: Messages are persistently stored on disk using append-only log files (`.bin`) and indexes (`.idx`) for fast lookups.
- **Offset Management**: Consumer offsets are tracked and persisted locally to allow consumers to resume processing from where they left off.
- **Distributed Consensus**: A built-in implementation of the Raft consensus algorithm ensures metadata consistency across broker nodes and enables leader election.
- **gRPC API**: Communication between clients (producers/consumers) and brokers is handled efficiently via gRPC.
- **Admin API**: Supports creating, deleting, and describing topics.

## Project Structure

```text
├── cmd
│   ├── broker     # Main entry point for the broker node
│   ├── consumer   # Example consumer client implementation
│   └── producer   # Example producer client implementation
├── internal
│   ├── broker     # Broker logic (Server, Manager, MessageStore, Rebalancing)
│   └── raft       # Implementation of the Raft consensus algorithm
├── pkg
│   └── client     # Client libraries for Producer and Consumer
├── proto          # Protobuf definitions for the gRPC API
└── scripts        # PowerShell scripts for various tests
```

## Prerequisites

- Go 1.21 or later
- Protocol Buffers (`protoc`)
- `protoc-gen-go` and `protoc-gen-go-grpc`
- Make (optional, but recommended for using the provided Makefile)

## Building the Project

The project includes a `Makefile` to simplify building and testing. To build all binaries (broker, producer, consumer):

```bash
make build
```

This will create the executables in the `bin/` directory.

## Running Locally

### 1. Start the Broker

You can start a single broker instance using the `make` command:

```bash
make run-broker
```
Or manually:
```bash
./bin/broker.exe -id 0 -addr :9092 -admin :8080 -data ./data
```

### 2. Run a Consumer

Start a consumer that subscribes to `test-topic` as part of `test-group`:

```bash
make run-consumer
```
Or manually:
```bash
./bin/consumer.exe -addr localhost:9092 -group test-group -topic test-topic
```

### 3. Run a Producer

Produce messages to the broker:

```bash
make run-producer
```
Or manually:
```bash
./bin/producer.exe -addr localhost:9092 -topic test-topic -key myKey -message "Hello Kafka-Mini"
```

## Advanced Usage

### Rebalancing & Consumer Groups

Kafka-Mini supports consumer group rebalancing. If you start multiple consumers with the same group ID, the broker will automatically distribute the topic's partitions among the active consumers using a Round-Robin assignment strategy. 

If a consumer disconnects, a session timeout (currently 10 seconds) will trigger a rebalance, reassigning the lost partitions to the remaining active consumers.

### Running Automated Tests

A suite of PowerShell integration tests is provided in the `scripts/` directory. These tests spin up local clusters and verify various behaviors.

To run tests using the Makefile:
```bash
make test-integration
make test-mutiple-key
make test-offsets
make test-resume
make test-timeout
```

## Protobuf Generation

If you modify the `.proto` files in the `proto/` directory, you need to regenerate the Go gRPC code:

```bash
make gen-proto
```
