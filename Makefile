FDB_CHECK := $(shell command -v fdbcli 2> /dev/null)
ROCKSDB_CHECK := $(shell echo "int main() { return 0; }" | gcc -lrocksdb -x c++ -o /dev/null - 2>/dev/null; echo $$?)

TAGS =

ifdef FDB_CHECK
	TAGS += foundationdb
endif

ifeq ($(ROCKSDB_CHECK), 0)
	TAGS += rocksdb
	CGO_CXXFLAGS := "${CGO_CXXFLAGS} -std=c++11"
	CGO_FLAGS += CGO_CXXFLAGS=$(CGO_CXXFLAGS)
endif

default: build

build: export GO111MODULE=on
build:
ifeq ($(TAGS),)
	$(CGO_FLAGS) go build -o bin/go-ycsb cmd/go-ycsb/*
else
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/go-ycsb cmd/go-ycsb/*
endif

check:
	golint -set_exit_status db/... cmd/... pkg/...

