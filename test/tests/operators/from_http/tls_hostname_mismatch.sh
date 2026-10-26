#!/bin/sh
# fixtures: [{http_server: {tls: true, tls_san: ["DNS:other.example"], expected_count: 0}}]
# timeout: 60
set -eu

# The certificate is issued by a trusted CA, but for a different host.
# shellcheck disable=SC2086
if $TENZIR_BINARY "from_http \"$HTTP_FIXTURE_METHOD_URL\", max_retry_count=0,
      tls={cacert: \"$HTTP_FIXTURE_TLS_CAFILE\"} { read_json }" \
  2>&1 | grep -o "certificate verify failed"; then
  :
else
  echo "pipeline did not fail certificate verification"
fi
