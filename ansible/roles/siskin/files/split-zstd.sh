#!/usr/bin/env bash
# split-zstd.sh — Fast line-splitting into zstd-compressed parts.
# Usage: some-command | ./split-zstd.sh [PREFIX] [N]

set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<EOF
Usage: ... | ${0##*/} [PREFIX] [N]

Split line-oriented stdin into zstd-compressed files.

  PREFIX   Output filename prefix (default: output)
  N        Lines per part (default: 10000)

Produces: PREFIX-part-0001.zst, PREFIX-part-0002.zst, ...
EOF
  exit 0
fi

PREFIX="${1:-output}"
N="${2:-10000}"

# Resolve output directory (same dir as prefix) so temp files live on the same device
OUTDIR="$(dirname "$PREFIX")"
BASE="$(basename "$PREFIX")"

cleanup() {
  rm -f "${OUTDIR}/${BASE}"-part-*.zst.tmp
  exit 1
}
trap cleanup INT TERM

awk -v outdir="$OUTDIR" -v base="$BASE" -v n="$N" '
  function open_next() {
    if (fd) {
      close(fd)
      cmd = sprintf("mv -- %s/%s-part-%04d.zst.tmp %s/%s-part-%04d.zst",
                     outdir, base, part, outdir, base, part)
      system(cmd)
    }
    part++
    tmp  = sprintf("%s/%s-part-%04d.zst.tmp", outdir, base, part)
    fd   = "zstd -q -T0 -o " tmp
  }
  BEGIN { part = 0; fd = "" }
  NR % n == 1 { open_next() }
  { print | fd }
  END {
    if (fd) {
      close(fd)
      cmd = sprintf("mv -- %s/%s-part-%04d.zst.tmp %s/%s-part-%04d.zst",
                     outdir, base, part, outdir, base, part)
      system(cmd)
    }
  }
'

trap - INT TERM
