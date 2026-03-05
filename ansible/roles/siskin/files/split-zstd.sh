#!/usr/bin/env bash
# split-zstd.sh — Fast line-splitting into zstd-compressed parts.
# Usage: some-command | ./split-zstd.sh [PREFIX] [N]

set -euo pipefail

PREFIX="${1:-output}"
N="${2:-10000000}"

awk -v prefix="$PREFIX" -v n="$N" '
  function open_next() {
    if (fd) close(fd)
    part++
    fd = "zstd -o " prefix "-part-" part ".zst"
  }
  BEGIN { part = 0; fd = "" }
  NR % n == 1 { open_next() }
  { print | fd }
  END { if (fd) close(fd) }
'
