FROM golang:alpine

WORKDIR /baxos

COPY go.mod .
COPY go.sum .

RUN mkdir client && \
    mkdir common && \
    mkdir configuration && \
    mkdir bin

COPY client client
COPY common common
COPY experiments/baxos-read-lease/values.yaml configuration

RUN go build -o /baxos/bin/baxos-client /baxos/client

CMD [ "sh", "-c", "/baxos/bin/baxos-client -id=\"${CLIENT_ID}\" -region=${CLIENT_REGION} -logFilePath=/logs/ -config=\"./configuration/values.yaml\" -requestType=request -debugOn -debugLevel=2 -testDuration=30 -batchSize=50 -arrivalRate=1000 -artificialLatency=0 -artificialLatencyMultiplier=10" ]
