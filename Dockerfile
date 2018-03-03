FROM golang:1.10-alpine 

RUN apk add --no-cache tini bash

ADD . /go/src/github.com/pingcap/go-ycsb 

RUN cd /go/src/github.com/pingcap/go-ycsb && go build -o /go-ycsb ./cmd/* 
RUN cp -rf /go/src/github.com/pingcap/go-ycsb/workloads /workloads

ENTRYPOINT ["/sbin/tini", "--"]

CMD [ "bash" ]