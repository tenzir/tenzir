#!/usr/bin/env bash

# This script runs ClangBuildAnalyzer on a clean build of tenzir
# Usage: ./scripts/analyze-build.sh [<additional-cmake-flags>]

set -e

# Force Clang compiler (required for -ftime-trace)
export CC="clang"
export CXX="clang++"

BUILD_DIR="build/cba"
TRACE_FILE="${BUILD_DIR}/time-trace-$(date +%s).json"
OUTPUT_FILE="${BUILD_DIR}/time-trace-$(date +%s).txt"

# Configure build with time traces enabled
echo "Configuring build with time traces enabled using $CC and $CXX..."
cmake "$@" -B "${BUILD_DIR}" \
  -DCMAKE_C_COMPILER="${CC}" \
  -DCMAKE_CXX_COMPILER="${CXX}" \
  -DTENZIR_ENABLE_TIME_TRACE:BOOL=ON

# Start capture, build, and analyze
echo "Starting time trace capture..."
ClangBuildAnalyzer --start "${BUILD_DIR}"

echo "Building with time traces..."
CCACHE_DISABLE=1 cmake --build "${BUILD_DIR}" -j | grep -v "Time trace\|tracing"

echo "Stopping capture and analyzing..."
ClangBuildAnalyzer --stop "${BUILD_DIR}" "${TRACE_FILE}"
ClangBuildAnalyzer --analyze "${TRACE_FILE}" | tee "${OUTPUT_FILE}"

echo "Analysis saved to ${OUTPUT_FILE}"
