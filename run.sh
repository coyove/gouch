#!/bin/sh

go run main.go db.go db_range.go db_get.go util.go node_info.go replicator.go model.go handlers.go "$@"
