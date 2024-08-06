# Distributed Content Searching System

This project is a distributed content searching system implemented using Go. The system consists of a bootstrap server and multiple nodes that communicate with each other to store and retrieve content.

## Technologies Used

- **Go**: For implementing the REST API and gRPC server.
- **OpenTelemetry**: For tracing HTTP and gRPC requests.
- **shift**: A fast lightweight HTTP router for Go.
- **Jaeger**: To visualize distributed tracing.
- **gRPC**: For peer-to-peer (P2P) communication between nodes.

## Prerequisites

- Go 1.20 or later
- Java 8 or later
- Docker (for running containers)

## Running the Jaeger UI

1. Run the Jaeger container:
    ```sh
    docker run -d --name jaeger \
      -e COLLECTOR_ZIPKIN_HTTP_PORT=9411 \
      -p 5775:5775/udp \
      -p 6831:6831/udp \
      -p 6832:6832/udp \
      -p 5778:5778 \
      -p 16686:16686 \
      -p 14268:14268 \
      -p 14250:14250 \
      -p 9411:9411 \
      jaegertracing/all-in-one:latest
    ```

2. Access the Jaeger UI by navigating to `http://localhost:16686` in your web browser.

## Running the Bootstrap Server

1. Navigate to the `misc/Bootstrap Server` directory:
    ```sh
    cd misc/Bootstrap Server
    ```

2. Compile and run the bootstrap server:
    ```sh
    java BootstrapServer.java
    ```

Alternatively, you can run the Bootstrap Server in Docker using the `yousuf64/bootstrap-server:1.0` image:

1. Pull the Docker image:
    ```sh
    docker pull yousuf64/bootstrap-server:1.0
    ```

2. Run the Docker container:
    ```sh
    docker run -d --name bootstrap-server -p 55555:55555 yousuf64/bootstrap-server:1.0
    ```

## Running the Nodes

1. Make sure Bootstrap Server and Jaeger UI is running.
2. Navigate to the project root directory.

3. Build the Go project:
    ```sh
    go build -o node
    ```

4. Run the node with the required program arguments:
    ```sh
    ./node --addr <addr> --dns <dns> --bootstrap <bootstrap> --username <username> --M <M> --ringSize <ringSize>
    ```

   Replace `<addr>`, `<dns>`, `<bootstrap>`, `<username>`, `<M>`, and `<ringSize>` with the appropriate values.

Alternatively, you can run the nodes using the Docker image `yousuf64/chord-kv:1.0`:

1. Pull the Docker image:
    ```sh
    docker pull yousuf64/chord-kv:1.0
    ```

2. Run the Docker container:
    ```sh
    docker run -d --name node -p <port>:<port> yousuf64/chord-kv --addr <addr> --dns <dns> --bootstrap <bootstrap> --username <username> --M <M> --ringSize <ringSize>
    ```
   
   Replace `<addr>`, `<dns>`, `<bootstrap>`, `<username>`, `<M>`, and `<ringSize>` with the appropriate values.

### Program Arguments

- `--addr`: The host address of the node which exposes both the REST API and the gRPC endpoint  (default: `localhost:8080`).
- `--dns`: The public DNS of the node (default: `--addr`).
- `--bootstrap`: The address of the bootstrap server (default: `localhost:55555`).
- `--username`: The username for the node (default: `sugarcane`).
- `--M`: The number of bits in the hash key (default: `3`).
- `--ringSize`: The size of the ring (default: `9`).

## REST API Endpoints

### Set Content

- **URL**: `/api/set`
- **Method**: `POST`
- **Description**: Stores the provided content associated with the given key in the distributed system.
- **Request Body**:
    ```json
    {
        "key": "exampleKey",
        "content": "exampleContent"
    }
    ```
- **Curl Command**:
    ```sh
    curl -X POST http://localhost:<http-port>/api/set -H "Content-Type: application/json" -d '{"key": "exampleKey", "content": "exampleContent"}'
    ```

### Get Content

- **URL**: `/api/get/:key`
- **Method**: `GET`
- **Description**: Retrieves the size and the hash of the content associated with the specified key from the distributed system.
- **Curl Command**:
    ```sh
    curl http://localhost:<http-port>/api/get/exampleKey
    ```

### Debug

- **URL**: `/api/debug`
- **Method**: `GET`
- **Description**: Provides debugging information about the current state of the node.
- **Curl Command**:
    ```sh
    curl http://localhost:<http-port>/api/debug
    ```

Replace `<http-port>` with the appropriate HTTP port number.

## gRPC Contract

The gRPC contract for peer-to-peer communication is available in the `peer.proto` file.