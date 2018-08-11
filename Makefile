FDB_CHECK := $(shell fdbcli -v 2>/dev/null; echo $$?)
ROCKSDB_CHECK := $(shell echo "int main() { return 0; }" | gcc -lrocksdb -x c++ -o /dev/null - 2>/dev/null; echo $$?)

TAGS = 

ifeq ($(FDB_CHECK), 0)
	TAGS += foundationdb
endif 

ifeq ($(ROCKSDB_CHECK), 0)
	TAGS += rocksdb
    CGO_CXXFLAGS := "${CGO_CXXFLAGS} -std=c++11"
    CGO_LDFLAGS := "${CGO_LDFLAGS} -g -O2 -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
    CGO_FLAGS += CGO_CXXFLAGS=$(CGO_CXXFLAGS) CGO_LDFLAGS=$(CGO_LDFLAGS)
endif

default: build

build:
ifeq ($(TAGS),)
	$(CGO_FLAGS) go build -o bin/go-ycsb cmd/go-ycsb/*
else
	$(CGO_FLAGS) go build -tags "$(TAGS)" -o bin/go-ycsb cmd/go-ycsb/*
endif

check:
	golint -set_exit_status db/... cmd/... pkg/...

update:
	which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
ifdef PKG
	dep ensure -add ${PKG}
else
	dep ensure -update
endif
	@echo "removing test files"
	dep prune
	bash ./clean_vendor.sh