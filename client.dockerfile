FROM golang:alpine

WORKDIR /baxos

COPY go.mod .
COPY go.sum .

RUN mkdir client
RUN mkdir common
RUN mkdir configuration
RUN mkdir bin

COPY client client
COPY common common
COPY configuration/local-configuration.yml configuration

RUN go build -o /baxos/bin/baxos-client /baxos/client

CMD [ "/baxos/bin/baxos-client" ]
