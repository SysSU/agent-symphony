#!/bin/sh
set -eu

if test "$#" -ge 5 && test "$1" = --batch; then
  pattern=$2 match=$3 error=$4
  shift 4
  for file do
    set +e
    grep -EI "$pattern" "$file" >/dev/null 2>&1
    status=$?
    set -e
    case $status in
      0) : >"$match" ;;
      1) ;;
      *) : >"$error" ;;
    esac
  done
  exit 0
fi

root=${1:-.}
while test "$root" != / && test "${root%/}" != "$root"; do
  root=${root%/}
done
case $root in
  -*) root=./$root ;;
esac
exclusion_root=$root
test "$root" != / || exclusion_root=
pattern='(github_pat_[A-Za-z0-9_]+|gh[pousr]_[A-Za-z0-9]{20,}|-----BEGIN (RSA |EC |OPENSSH )?PRIVATE'" KEY-----)"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-symphony-credential-scan.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
scanner=$(CDPATH= cd "$(dirname "$0")" && pwd)/$(basename "$0")

if ! find "$root" \( -path "$exclusion_root/.git" -o -path "$exclusion_root/.worktrees" -o -path "$exclusion_root/dist" \) -prune -o -type f \
  -exec "$scanner" --batch "$pattern" "$tmp/match" "$tmp/error" {} + 2>/dev/null; then
  : >"$tmp/error"
fi
test ! -e "$tmp/error" || exit 2
test ! -e "$tmp/match" || exit 1
