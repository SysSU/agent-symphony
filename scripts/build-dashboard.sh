#!/bin/sh
set -eu

root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
dashboard="$root/cmd/agent-symphony/dashboard"
npm --prefix "$dashboard" ci
npm --prefix "$dashboard" run lint
npm --prefix "$dashboard" run test
npm --prefix "$dashboard" run build
touch "$dashboard/out/.gitkeep"
