# First stage: Build the Java application
FROM amazoncorretto:8-alpine3.17-jdk AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the source code
COPY . .

# Compile the Java application
RUN javac BootstrapServer.java Neighbour.java

# Second stage: Create a minimal image with the Java application
FROM amazoncorretto:8-alpine3.17-jre

# Set the working directory inside the container
WORKDIR /app

# Copy the compiled Java application from the builder stage
COPY --from=builder /app/ .

# Expose the port the application runs on
EXPOSE 55555/udp

# Command to run the Java application
ENTRYPOINT ["java", "BootstrapServer"]