#!/bin/bash

protoc --go_out=./ --go_opt=paths=source_relative --go-brpc_out=./ --go-brpc_opt=paths=source_relative  leveldb.proto
protoc --go_out=. --go_opt=paths=source_relative  *.proto
