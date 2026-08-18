#!/bin/sh
set -eu

root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
cd "$root"

unformatted=$(git ls-files -z -- '*.go' | xargs -0 gofmt -l)
if test -n "$unformatted"; then
  echo "gofmt required:" >&2
  echo "$unformatted" >&2
  exit 1
fi

go vet ./...
go tool staticcheck ./...
npm --prefix cmd/agent-symphony/dashboard ci
npm --prefix cmd/agent-symphony/dashboard run lint
