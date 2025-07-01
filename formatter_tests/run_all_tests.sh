#!/bin/bash

# TQL Formatter Test Runner
# This script runs all formatter tests and compares outputs

echo "🧪 Running TQL Pretty Printer Formatter Tests..."
echo "================================================"

cd "$(dirname "$0")/.."
TENZIR="./build/clang/debug/bin/tenzir"

if [ ! -f "$TENZIR" ]; then
  echo "❌ Error: Tenzir executable not found at $TENZIR"
  exit 1
fi

test_count=0
passed_count=0

for tql_file in formatter_tests/*.tql; do
  if [ -f "$tql_file" ]; then
    test_name=$(basename "$tql_file" .tql)
    expected_file="formatter_tests/${test_name}.txt"

    echo -n "Testing $test_name... "
    test_count=$((test_count + 1))

    if [ -f "$expected_file" ]; then
      # Run formatter and compare with expected output
      if $TENZIR --dump-formatted -f "$tql_file" | diff -q - "$expected_file" >/dev/null 2>&1; then
        echo "✅ PASS"
        passed_count=$((passed_count + 1))
      else
        echo "❌ FAIL"
        echo "   Expected output differs from actual output"
        echo "   Run: $TENZIR --dump-formatted -f $tql_file | diff - $expected_file"
      fi
    else
      echo "⚠️  SKIP (no expected output file)"
    fi
  fi
done

echo ""
echo "📊 Test Results: $passed_count/$test_count tests passed"

if [ $passed_count -eq $test_count ]; then
  echo "🎉 All tests passed! The TQL formatter is working perfectly."
  exit 0
else
  echo "💥 Some tests failed. Please review the differences above."
  exit 1
fi
