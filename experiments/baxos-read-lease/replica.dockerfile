FROM golang:alpine

WORKDIR /baxos

COPY go.mod .
COPY go.sum .

RUN mkdir replica && \
    mkdir common && \
    mkdir configuration && \
    mkdir bin

COPY replica replica
COPY common common
COPY experiments/baxos-read-lease/values.yaml configuration

# Install protoc compiler and other dependencies
RUN apk add --no-cache git protobuf protobuf-dev protoc

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
    export PATH="$PATH:$(go env GOPATH)/bin" && \
    protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    common/definitions.proto

RUN go build -o /baxos/bin/baxos-replica /baxos/replica

CMD [ "sh", "-c", "/baxos/bin/baxos-replica -id=\"${REPLICA_ID}\" -region=${REPLICA_REGION} -logFilePath=/logs/ -config=\"./configuration/values.yaml\" -debugOn -debugLevel=1 -roundTripTime=2000 -artificialLatency=0 -artificialLatencyMultiplier=10" ]
