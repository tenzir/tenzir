#!/usr/bin/env bash
set -euo pipefail

# Script to process code coverage data from integration tests

# Parse command line arguments
VERBOSE=${VERBOSE:-0}
while [[ $# -gt 0 ]]; do
  case $1 in
  --coverage-dir=*)
    COVERAGE_DIR="${1#*=}"
    shift
    ;;
  --source-dir=*)
    SOURCE_DIR="${1#*=}"
    shift
    ;;
  --binary-dir=*)
    BINARY_DIR="${1#*=}"
    shift
    ;;
  --tenzir-binary=*)
    TENZIR_BINARY="${1#*=}"
    shift
    ;;
  --profdata=*)
    PROFDATA="${1#*=}"
    shift
    ;;
  --llvm-profdata=*)
    LLVM_PROFDATA="${1#*=}"
    shift
    ;;
  --llvm-cov=*)
    LLVM_COV="${1#*=}"
    shift
    ;;
  --verbose)
    VERBOSE=1
    shift
    ;;
  -v)
    VERBOSE=1
    shift
    ;;
  *)
    echo "Unknown option: $1"
    exit 1
    ;;
  esac
done

# Ensure required parameters are set
if [ -z "$COVERAGE_DIR" ]; then
  echo "Error: --coverage-dir is required" >&2
  exit 1
fi

if [ -z "$SOURCE_DIR" ]; then
  echo "Error: --source-dir is required" >&2
  exit 1
fi

if [ -z "$BINARY_DIR" ]; then
  echo "Error: --binary-dir is required" >&2
  exit 1
fi

if [ -z "$TENZIR_BINARY" ]; then
  echo "Error: --tenzir-binary is required" >&2
  exit 1
fi

if [ -z "$LLVM_PROFDATA" ]; then
  echo "Error: --llvm-profdata is required" >&2
  exit 1
fi

if [ -z "$LLVM_COV" ]; then
  echo "Error: --llvm-cov is required" >&2
  exit 1
fi

# Log function that respects verbosity
log() {
  if [ $VERBOSE -eq 1 ]; then
    echo "$1" >&2
  fi
}

run() {
  if [ $VERBOSE -eq 1 ]; then
    "$@"
  else
    "$@" >/dev/null 2>&1
  fi
}

# Compute the relative path of BINARY_DIR inside SOURCE_DIR
BUILD_DIR_RELATIVE=$(python3 -c "import os.path; print(os.path.relpath('${BINARY_DIR}', '${SOURCE_DIR}'))")

echo "Processing coverage data..." >&2

# Create coverage directory if it doesn't exist
mkdir -p "${COVERAGE_DIR}"

# Check if we have any profraw files
if [ -z "$(find "${COVERAGE_DIR}" -name "*.profraw" 2>/dev/null)" ]; then
  echo "No coverage data found in ${COVERAGE_DIR}" >&2
  exit 0
fi

# Process coverage data
if [ -z "${PROFDATA:-}" ]; then
  PROFDATA="${COVERAGE_DIR}/integration.profdata"
  echo "Merging coverage data to ${PROFDATA}..." >&2
  if [ $VERBOSE -eq 1 ]; then
    "${LLVM_PROFDATA}" merge -sparse "${COVERAGE_DIR}"/*.profraw -o "${PROFDATA}"
  else
    "${LLVM_PROFDATA}" merge -sparse "${COVERAGE_DIR}"/*.profraw -o "${PROFDATA}" >/dev/null 2>&1
  fi
else
  log "Using existing profdata file: ${PROFDATA}"
fi

# Find instrumented binaries
echo "Finding instrumented binaries..." >&2

# Start with the tenzir binary
if [ ! -f "${TENZIR_BINARY}" ]; then
  echo "Error: Tenzir binary not found at ${TENZIR_BINARY}" >&2
  exit 1
fi

log "Using tenzir binary: ${TENZIR_BINARY}"
OBJECTS="-object=${TENZIR_BINARY}"

# Add other shared libraries that might be instrumented
log "Scanning for additional instrumented libraries..."
for EXTENSION in dylib so; do
  find "${BINARY_DIR}" -name "*.${EXTENSION}" | while read LIB_PATH; do
    if grep -q "__llvm_covmap" <(otool -l "${LIB_PATH}" 2>/dev/null) ||
      grep -q "__llvm_covmap" <(objdump -h "${LIB_PATH}" 2>/dev/null); then
      log "Found instrumented library: ${LIB_PATH}"
      OBJECTS="${OBJECTS} -object=${LIB_PATH}"
    fi
  done
done

# Print the final objects if verbose
log "Coverage objects: ${OBJECTS}"

# Generate coverage report

log "Generating coverage report..."
run ${LLVM_COV} report ${OBJECTS} \
  -instr-profile="${PROFDATA}" \
  -path-equivalence="${BINARY_DIR},${SOURCE_DIR}" \
  -path-equivalence="${BUILD_DIR_RELATIVE},${SOURCE_DIR}"

# Generate HTML report
log "Generating HTML report..."
run ${LLVM_COV} show ${OBJECTS} \
  -instr-profile="${PROFDATA}" \
  -path-equivalence="${BINARY_DIR},${SOURCE_DIR}" \
  -path-equivalence="${BUILD_DIR_RELATIVE},${SOURCE_DIR}" \
  -format=html \
  -o "${COVERAGE_DIR}/integration-html"

echo "Coverage report available at: ${COVERAGE_DIR}/integration-html/index.html" >&2

# Check for warnings and show them if in verbose mode
WARNINGS=$(${LLVM_COV} report ${OBJECTS} \
  -instr-profile="${PROFDATA}" \
  -path-equivalence="${BINARY_DIR},${SOURCE_DIR}" \
  -path-equivalence="${BUILD_DIR_RELATIVE},${SOURCE_DIR}" 2>&1 | grep -i "warning:" || true)

if [ -n "$WARNINGS" ]; then
  if [ $VERBOSE -eq 1 ]; then
    log "Warnings in coverage report:"
    log "$WARNINGS"
    log "This might indicate mismatched binaries or incomplete data."
    log "Tips to resolve:"
    log "1. Make sure all binaries are built with -fprofile-instr-generate -fcoverage-mapping"
    log "2. Check that you're using the same compiler version for all components"
    log "3. Rebuild the entire project with coverage enabled"
  else
    echo "Note: Some coverage warnings were detected. Run with --verbose to see details." >&2
  fi
fi
