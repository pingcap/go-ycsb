FROM ubuntu:18.04

ENV GOPATH /go

RUN apt-get update \
 && apt-get install -y \
                wget \
                dpkg \
                python \
                golang \
                git \
                net-tools \
 && wget https://www.foundationdb.org/downloads/5.1.7/ubuntu/installers/foundationdb-clients_5.1.7-1_amd64.deb \
 && dpkg -i foundationdb-clients_5.1.7-1_amd64.deb \
 && go get -u github.com/golang/dep/cmd/dep

ADD . /go/src/github.com/pingcap/go-ycsb

WORKDIR /go/src/github.com/pingcap/go-ycsb

RUN go build -tags "foundationdb" -o /go-ycsb ./cmd/*

FROM ubuntu:18.04

RUN apt-get update \
 && apt-get install -y dpkg

COPY --from=0 /foundationdb-clients_5.1.7-1_amd64.deb /foundationdb-clients_5.1.7-1_amd64.deb
RUN dpkg -i foundationdb-clients_5.1.7-1_amd64.deb

COPY --from=0 /go-ycsb /go-ycsb

ADD workloads /workloads

ENTRYPOINT [ "/go-ycsb" ]
