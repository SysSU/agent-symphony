#!/bin/sh
set -eu

version=${1:?usage: scripts/release.sh VERSION [OUTPUT_DIR]}
out=${2:-dist}
case "$version" in *[!0-9A-Za-z._-]*|'') echo "invalid version: $version" >&2; exit 2;; esac
test ! -e "$out" || { echo "output already exists: $out" >&2; exit 1; }
scripts/build-dashboard.sh
trap 'rm -rf cmd/agent-symphony/dashboard/node_modules' EXIT HUP INT TERM
mkdir -p "$out"
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-0} go run ./tools/release "$version" "$out"
