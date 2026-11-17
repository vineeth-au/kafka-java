# Kafka Broker — Java

A simplified implementation of an Apache Kafka-compatible broker, written in Java as part of the [CodeCrafters Build Your Own Kafka](https://codecrafters.io/challenges/kafka) challenge. The primary objective of this project is to understand how Kafka works internally by implementing parts of its binary wire protocol, handling client requests, and constructing protocol-compliant responses.

## Understanding Kafka's Wire Protocol
A wire protocol defines how applications exchange and interpret data over a network. TCP provides a reliable, ordered stream of bytes, but it does not define application-level message boundaries or the meaning of those bytes.

Kafka uses its own binary wire protocol to define how clients and brokers communicate. A Kafka request follows this general structure:


```text
+------------------------------+
| Message Size (4 bytes)       |
+------------------------------+
| Request Header               |
|  - API Key                   |
|  - API Version               |
|  - Correlation ID            |
|  - Client ID                 |
|  - Tagged Fields (optional)  |
+------------------------------+
| Request Body                 |
|  - API-specific fields       |
+------------------------------+
```

The message size specifies the number of bytes following the four-byte size field. The request header identifies the operation, its version, and the request's correlation ID. The request body contains operation-specific data. Kafka responses use a similar structure, containing a message size, response header, and API-specific response body. The precise field layout depends on the API version and the corresponding header version.

For further details, see the [Apache Kafka Protocol Specification](https://kafka.apache.org/protocol.html).

# How to Run

### Prerequisites
- Java 25
- Apache Maven

### Running the Broker
Run the following command from the project root:
```bash
./your_program.sh
```
The entry point for the Kafka broker is `src/main/java/Main.java`.