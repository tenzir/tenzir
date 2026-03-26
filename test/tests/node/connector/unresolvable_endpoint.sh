# runner: shell
# timeout: 15
# Regression test: tenzir must not segfault when the configured
# endpoint hostname cannot be resolved.
exit_code=0
$TENZIR_BINARY --bare-mode --endpoint=this-host-does-not-exist.invalid:1234 \
  --tenzir.connection-timeout=5s --tenzir.connection-retry-delay=0ms \
  'from {}' 2>/dev/null || exit_code=$?
# A segfault yields exit code 139 (128 + SIGSEGV). Any non-signal error
# means the crash is fixed.
if [ "$exit_code" -ge 128 ]; then
  echo "process killed by signal $((exit_code - 128))"
  exit 1
fi
# tenzir should have exited with an error (not zero).
test "$exit_code" -ne 0
