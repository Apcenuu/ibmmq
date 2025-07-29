FROM golang:1.19 AS build
WORKDIR /app

COPY ./ibmmq-sdk_9.3.0.0_amd64.deb /opt/
COPY ./ibmmq-runtime_9.3.0.0_amd64.deb /opt/

RUN apt install /opt/ibmmq-*.deb

COPY ./producer.go producer.go

COPY ./go.mod go.mod
COPY ./go.sum go.sum
RUN go mod download

COPY ./producer.go producer.go

RUN go build -a -o main .

FROM golang:latest as app
WORKDIR /app
COPY --from=build /app/main /app/main
COPY --from=build /opt/mqm /opt/mqm

CMD ["/app/main"]