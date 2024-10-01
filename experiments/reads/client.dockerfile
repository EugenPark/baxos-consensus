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
COPY experiments/reads/values.yaml configuration

RUN go build -o /baxos/bin/baxos-client /baxos/client

CMD [ "sh", "-c", "/baxos/bin/baxos-client -id=${CLIENT_ID} -logFilePath=/logs/ -config=./configuration/values.yaml" ]
