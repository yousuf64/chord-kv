# First stage: Build the Go binary
FROM golang:1.22 AS builder
LABEL authors="yousuf64"

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download && go mod verify

# Copy the source code
COPY . .

# Build the Go binary
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /usr/local/bin/app .

# Second stage: Create a minimal image with the Go binary
FROM alpine:latest

# Set the working directory inside the container
#WORKDIR /root/

# Copy the Go binary from the builder stage
COPY --from=builder /usr/local/bin/app /bin/app

## Expose the port the application runs on
EXPOSE 8080

# Command to run the Go binary
ENTRYPOINT ["/bin/app"]