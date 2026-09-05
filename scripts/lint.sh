#!/bin/sh
set -eu

root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
cd "$root"

unformatted=$(git ls-files -z --cached --others --exclude-standard -- '*.go' | xargs -0 sh -c 'for path do test ! -f "$path" || gofmt -l "$path"; done' sh)
if test -n "$unformatted"; then
  echo "gofmt required:" >&2
  echo "$unformatted" >&2
  exit 1
fi

go vet ./...
go tool staticcheck ./...
npm --prefix cmd/agent-symphony/dashboard ci
npm --prefix cmd/agent-symphony/dashboard run lint
