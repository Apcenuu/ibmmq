FROM golang:1.19 AS build
WORKDIR /app

COPY ./ibmmq-sdk_9.3.0.0_amd64.deb /opt/
COPY ./ibmmq-runtime_9.3.0.0_amd64.deb /opt/

RUN apt install /opt/ibmmq-*.deb

COPY ./go.mod go.mod
COPY ./go.sum go.sum
RUN go mod download
COPY ./ibmConsumer.go ibmConsumer.go
COPY ./Ibm Ibm
COPY ./Rabbit Rabbit
COPY ./Translator Translator


RUN go build -a -o main .


FROM ubuntu:22.04 as app
WORKDIR /app

COPY --from=build /opt/mqm/lib64/libmqm_r.so /opt/mqm/lib64/libmqm_r.so
COPY --from=build /opt/mqm/lib64/libmqe_r.so /opt/mqm/lib64/libmqe_r.so
COPY --from=build /opt/mqm/samp/ccsid_part2.tbl /opt/mqm/samp/ccsid_part2.tbl
COPY --from=build /opt/mqm/samp/ccsid.tbl /opt/mqm/samp/ccsid.tbl
COPY --from=build /app/main /app/main

CMD ["/app/main"]
