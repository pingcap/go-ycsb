FROM golang:1.10-alpine 

RUN apk add --no-cache bash

ADD . /go/src/github.com/pingcap/go-ycsb 

RUN cd /go/src/github.com/pingcap/go-ycsb && go build -o /go-ycsb ./cmd/* 
RUN cp -rf /go/src/github.com/pingcap/go-ycsb/workloads /workloads

# Use `docker exec -it go-ycsb bash` in another terminal to proceed.
# hack for keep this container running
CMD tail -f /dev/null