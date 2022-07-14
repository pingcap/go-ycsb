FROM ubuntu:jammy

ENV GOPATH /go

RUN apt update && apt install -y golang git wget \
 && wget -O - https://tigris.dev/cli-linux | tar -xz -C /usr/local/bin \
 && chmod +x /usr/local/bin/tigris

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
 && chmod +x /usr/local/bin/dumb-init

RUN wget https://github.com/apple/foundationdb/releases/download/7.1.7/foundationdb-clients_7.1.7-1_amd64.deb \
 && dpkg -i foundationdb*.deb

RUN mkdir -p /go/src/github.com/pingcap/go-ycsb
WORKDIR /go/src/github.com/pingcap/go-ycsb

COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN GO111MODULE=on go build -tags "foundationdb tigris" -o /go-ycsb ./cmd/*

FROM ubuntu:jammy

COPY --from=0 /go-ycsb /go-ycsb
COPY --from=0 /usr/local/bin/dumb-init /usr/local/bin/dumb-init
COPY --from=0 /usr/local/bin/tigris /usr/local/bin/tigris

RUN apt update && apt install -y wget && apt-get purge -y --auto-remove \
 && wget https://github.com/apple/foundationdb/releases/download/7.1.7/foundationdb-clients_7.1.7-1_amd64.deb \
 && dpkg -i foundationdb*.deb

ADD workloads /workloads
ADD run.sh /run.sh

EXPOSE 6060

ENTRYPOINT [ "/run.sh" ]
