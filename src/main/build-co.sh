#!/usr/bin/env bash
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrcoordinator.go pg-*.txt