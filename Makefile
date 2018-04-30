FDBCLI_VERSION := $(shell fdbcli -v 2>/dev/null)
TAGS=

default: build

build:
ifdef FDBCLI_VERSION
	go build -tags "foundationdb" -o bin/go-ycsb cmd/go-ycsb/*
else 
	go build -o bin/go-ycsb cmd/go-ycsb/*
endif

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