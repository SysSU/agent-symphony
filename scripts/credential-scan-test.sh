#!/bin/sh
set -eu

if test "${0##*/}" = find; then
  test "$#" -eq 23 &&
    test "$1" = / && test "$2" = '(' && test "$3" = -path && test "$4" = /.git &&
    test "$5" = -o && test "$6" = -path && test "$7" = /.worktrees &&
    test "$8" = -o && test "$9" = -path && test "${10}" = /dist && test "${11}" = ')' &&
    test "${12}" = -prune && test "${13}" = -o && test "${14}" = -type && test "${15}" = f &&
    test "${16}" = -exec && test "${17}" = "$scanner" && test "${18}" = --batch &&
    test "${19}" = "$pattern" &&
    case ${20} in "$test_tmp"/agent-symphony-credential-scan.*/match) true;; *) false;; esac &&
    case ${21} in "$test_tmp"/agent-symphony-credential-scan.*/error) true;; *) false;; esac &&
    test "${22}" = '{}' && test "${23}" = + || exit 1
  exit
fi

if test "${0##*/}" = grep; then
  for arg do
    case $arg in
      -*q*) exit 0 ;;
    esac
  done
  printf 'suppressed grep output\n'
  printf 'suppressed grep error\n' >&2
  exit 2
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-symphony-credential-scan-test.XXXXXX")
trap 'chmod -R u+rwx "$tmp" 2>/dev/null || :; rm -rf "$tmp"' EXIT HUP INT TERM
scanner=${SCANNER_UNDER_TEST:-$(CDPATH= cd "$(dirname "$0")" && pwd)/credential-scan.sh}
canary=ghp_1234567890
canary=${canary}1234567890
mkdir "$tmp/find-bin" "$tmp/grep-bin"
cp "$0" "$tmp/find-bin/find"
pattern='(github_pat_[A-Za-z0-9_]+|gh[pousr]_[A-Za-z0-9]{20,}|-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----)'
scanner=$scanner pattern=$pattern test_tmp=$tmp PATH="$tmp/find-bin:$PATH" TMPDIR="$tmp" "$scanner" /
if scanner=$scanner pattern=$pattern test_tmp=$tmp "$tmp/find-bin/find" / WRONG ROOT ARGS; then
  exit 1
fi
mkdir -p "$tmp/clean/.git" "$tmp/clean/.worktrees" "$tmp/clean/dist"
: >"$tmp/clean/ordinary"
printf '%s\n' "$canary" >"$tmp/clean/.git/ignored"
printf '%s\n' "$canary" >"$tmp/clean/.worktrees/ignored"
printf '%s\n' "$canary" >"$tmp/clean/dist/ignored"
"$scanner" "$tmp/clean"

mkdir -p "$tmp/root with spaces/.git" "$tmp/root with spaces/.worktrees" "$tmp/root with spaces/dist"
printf '%s\n' "$canary" >"$tmp/root with spaces/.git/ignored"
printf '%s\n' "$canary" >"$tmp/root with spaces/.worktrees/ignored"
printf '%s\n' "$canary" >"$tmp/root with spaces/dist/ignored"
for root in "$tmp/root with spaces" "$tmp/root with spaces/" "$tmp/root with spaces///"; do
  "$scanner" "$root"
done
(cd "$tmp" && "$scanner" ./"root with spaces"/)

mkdir "$tmp/-root" "$tmp/--batch"
(cd "$tmp" && "$scanner" -root && "$scanner" --batch)

mkdir -p "$tmp/root with spaces/nested/.git" "$tmp/root with spaces/nested/.worktrees" "$tmp/root with spaces/nested/dist"
for path in nested/.git/canary nested/.worktrees/canary nested/dist/canary; do
  printf '%s\n' "$canary" >"$tmp/root with spaces/$path"
done
for root in "$tmp/root with spaces" "$tmp/root with spaces/" "$tmp/root with spaces///"; do
  set +e
  output=$("$scanner" "$root" 2>&1)
  status=$?
  set -e
  test "$status" -eq 1
  test -z "$output"
done

mkdir "$tmp/many"
i=0
while test "$i" -lt 400; do
  : >"$tmp/many/file-$i"
  i=$((i + 1))
done
"$scanner" "$tmp/many"

mkdir "$tmp/match"
printf '%s\n' "$canary" >"$tmp/match/ignored.env"
set +e
output=$("$scanner" "$tmp/match" 2>&1)
status=$?
set -e
test "$status" -eq 1
test -z "$output"

mkdir -p "$tmp/error/denied"
: >"$tmp/error/denied/file"
chmod 000 "$tmp/error/denied"
if ! test -r "$tmp/error/denied/file"; then
  set +e
  output=$("$scanner" "$tmp/error" 2>&1)
  status=$?
  set -e
  test "$status" -eq 2
  test -z "$output"
fi

# Exercise aggregation across separate batches without depending on find's batch size.
mkdir "$tmp/batches"
: >"$tmp/batches/clean"
printf '%s\n' "$canary" >"$tmp/batches/match"
pattern='ghp_[A-Za-z0-9]{20,}'
"$scanner" --batch "$pattern" "$tmp/batches/found" "$tmp/batches/error" "$tmp/batches/clean"
"$scanner" --batch "$pattern" "$tmp/batches/found" "$tmp/batches/error" "$tmp/batches/match"
"$scanner" --batch "$pattern" "$tmp/batches/found" "$tmp/batches/error" "$tmp/batches/missing"
test -e "$tmp/batches/found"
test -e "$tmp/batches/error"

# A match followed by a read error in one file must be an error, not a match.
mkdir "$tmp/same-file"
cp "$0" "$tmp/grep-bin/grep"
: >"$tmp/same-file/input"
set +e
output=$(PATH="$tmp/grep-bin:$PATH" "$scanner" "$tmp/same-file" 2>&1)
status=$?
set -e
test "$status" -eq 2
test -z "$output"

if test "${MUTATION_PROOF:-}" != 1; then
  sed 's/grep -EI /grep -qEI /' "$scanner" >"$tmp/credential-scan-mutant.sh"
  chmod +x "$tmp/credential-scan-mutant.sh"
  if MUTATION_PROOF=1 SCANNER_UNDER_TEST="$tmp/credential-scan-mutant.sh" "$0"; then
    exit 1
  fi
fi
