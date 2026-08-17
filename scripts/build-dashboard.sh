#!/bin/sh
set -eu

root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
"$root/scripts/lint.sh"
cd "$root/cmd/agent-symphony/dashboard"
npm run test
npm run build
