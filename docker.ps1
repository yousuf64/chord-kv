$version = 0.1
$network = 'chord_network'

# Build image
#docker build -t chord-kv:${version} .

#docker network create ${network}

# Run Jaeger
#docker run -d --name jaeger `
#    --net ${network} `
#    -e COLLECTOR_ZIPKIN_HTTP_PORT=9411 `
#    -p 5775:5775/udp `
#    -p 6831:6831/udp `
#    -p 6832:6832/udp `
#    -p 5778:5778 `
#    -p 16686:16686 `
#    -p 14268:14268 `
#    -p 14250:14250 `
#    -p 9411:9411 `
#jaegertracing/all-in-one:latest

# Run bootstrap server
#docker run --name bootstrap --net ${network}  -p 55555:55555/udp bootstrap-server:1.0

# Run container
$hostIp = 'localhost'
$hostPort = 7071
$bsIp = 'bootstrap' # Bootstrap IP
$bsPort = 55555 # Bootstrap port
$jaegerEndpoint = 'http://jaeger:14268/api/traces'
$username = 'jellyfish'
$M = 5
$ringSize = 32

docker run  `
    --name ${username} `
    --net ${network} `
    -p 7071:7071 `
    -e OTEL_EXPORTER_JAEGER_ENDPOINT=${jaegerEndpoint} `
    chord-kv:${version} `
    `
    --addr=${hostIp}:${hostPort} `
    --bootstrap=${bsIp}:${bsPort} `
    --username=${username} `
    --M=${M} `
    --ringSize=${ringSize}
