#!/bin/sh
set -eu

cd "$(dirname "$0")/../cmd/agent-symphony/dashboard"
npm ci
npm run build
