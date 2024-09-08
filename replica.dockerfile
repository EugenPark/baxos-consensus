FROM golang:alpine

WORKDIR /baxos

COPY go.mod .
COPY go.sum .

RUN mkdir replica
RUN mkdir common
RUN mkdir configuration

COPY replica replica
COPY common common
COPY configuration/local-configuration.yml configuration

RUN go build -o /baxos/bin/baxos-replica /baxos/replica

CMD [ "/baxos/bin/baxos-replica" ]
