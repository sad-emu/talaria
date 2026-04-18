#!/usr/bin/env bash
# unit_test.sh — run all talaria unit tests
set -euo pipefail

GOPATH="${GOPATH:-$(go env GOPATH)}"
GOCACHE="${GOCACHE:-$(go env GOCACHE)}"

export GOPATH GOCACHE

echo "==> Running talaria unit tests"
GOPATH="$GOPATH" GOCACHE="$GOCACHE" go test -timeout 60s -count=1 ./... "$@"
echo "==> All tests passed"
