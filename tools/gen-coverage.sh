#!/usr/bin/env bash
# gen-coverage.sh — capture and generate a filtered code coverage report.
#
# Usage:  ./tools/gen-coverage.sh [build_dir] [output_dir]
#
# Requires: lcov, genhtml (apt install lcov)
# Excludes: third_party/*, test/*, bench/*, examples/*, generator/*,
#           include/ (header-only), build/* — only project implementation
#           code (urpc/core/source, urpc/api/source) is counted.

set -euo pipefail

BUILD_DIR="${1:-build/coverage}"
OUT_DIR="${2:-coverage-report}"

INFO_ALL="$OUT_DIR/coverage-all.info"
INFO_URPC="$OUT_DIR/coverage-urpc.info"

if [ ! -d "$BUILD_DIR" ]; then
  echo "error: build directory '$BUILD_DIR' not found" >&2
  echo "hint: cmake --preset coverage && cmake --build --preset coverage" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

# --- capture -----------------------------------------------------------------
echo "==> capturing coverage data from $BUILD_DIR"
lcov --capture \
  --directory "$BUILD_DIR" \
  --output-file "$INFO_ALL" \
  --ignore-errors inconsistent,mismatch,unused,empty,no-marked,unmapped \
  2>&1 | grep -v "^lcov: WARNING:" || true

if [ ! -s "$INFO_ALL" ]; then
  echo "error: lcov produced an empty trace file (no .gcda data?)" >&2
  echo "hint: run the test suite first: ctest --test-dir $BUILD_DIR" >&2
  exit 1
fi

# --- filter: remove everything that is NOT project implementation code ------
# Single --remove pass (avoids the two-step extract that loses api files
# when the intermediate trace only matched one pattern).
echo "==> filtering to project implementation code"
lcov --remove "$INFO_ALL" \
  '*/third_party/*' \
  '*/test/*' \
  '*/bench/*' \
  '*/examples/*' \
  '*/generator/*' \
  '*/include/*' \
  '*/build/*' \
  --output-file "$INFO_URPC" \
  --ignore-errors inconsistent,mismatch,unused,empty,no-marked,unmapped \
  2>/dev/null

# --- summary -----------------------------------------------------------------
echo ""
echo "==> coverage summary (project source only)"
lcov --summary "$INFO_URPC"

# --- HTML report ----------------------------------------------------------------
echo "==> generating HTML report: $OUT_DIR/coverage-html/"
rm -rf "$OUT_DIR/coverage-html"
genhtml "$INFO_URPC" \
  --output-directory "$OUT_DIR/coverage-html" \
  --title "urpc coverage" \
  --show-details \
  --legend

echo ""
echo "==> done. open $OUT_DIR/coverage-html/index.html to view the report"
