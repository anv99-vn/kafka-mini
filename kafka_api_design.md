# Kafka-mini API Design

This document outlines the core APIs required to build a message queue system similar to Apache Kafka. The system follows the basic principles of topics, partitions, and offsets.

## Core APIs

### 1. Producer API
The Producer is responsible for publishing messages to topics.

| Method | Description |
| :--- | :--- |
| `send(topic: String, message: Bytes)` | Sends a message to a specific topic. |
| `send(topic: String, key: Bytes, message: Bytes)` | Sends a message with a key for custom partitioning. |
| `close()` | Shut down the producer. |

### 2. Consumer API
The Consumer reads messages from topics. It requires a `groupId` to manage offsets and support scaling.

| Method | Description |
| :--- | :--- |
| `Consumer(groupId: String)` | Initialize a consumer with a specific group ID. |
| `subscribe(topics: List<String>)` | Subscribe to a set of topics. |
| `poll(timeout: Duration)` | Retrieve a batch of messages from the subscribed topics. |
| `commit(offsets: Map<TopicPartition, Offset>)` | Manually commit processed offsets. |
| `seek(partition: TopicPartition, offset: Offset)` | Move the read pointer to a specific offset. |
| `close()` | Shut down the consumer. |

### 3. Admin API
Used for managing the cluster state.

| Method | Description |
| :--- | :--- |
| `createTopic(name: String, partitions: Int, rf: Int)` | Create a new topic with partitions and replication. |
| `deleteTopic(name: String)` | Delete an existing topic. |
| `listTopics()` | List all available topics. |
| `describeTopic(name: String)` | Get details about partitions and replicas for a topic. |

### 4. Storage/Broker API (Internal)
The low-level API for data persistence on the broker side.

| Method | Description |
| :--- | :--- |
| `append(partition: Partition, data: Bytes)` | Append data to the end of the partition log. |
| `read(partition: Partition, startOffset: Offset, size: Int)` | Read a chunk of data from a specific offset. |

---
*Created by Antigravity*
