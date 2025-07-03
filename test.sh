cd /Users/doloresh4ze/code/tenzir/tenzir
for test_file in $(find tenzir/tests/format -name '*.tql'); do
    echo "Testing: $test_file"
    ./build/clang/debug/bin/tenzir --format "$(<"$test_file")" > /tmp/format_output.txt
    if diff --strip-trailing-cr -u "${test_file%.*}.txt" /tmp/format_output.txt; then
        echo "✓ PASS: $test_file"
    else
        echo "✗ FAIL: $test_file"
    fi
done
