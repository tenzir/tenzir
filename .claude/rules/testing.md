---
paths:
  - "test/**/*"
---

## Runners

The following project-specific test runners exist in `test/runners/`:

- `lexer` - token stream (`tenzir --dump-tokens`)
- `ast` - abstract syntax tree (`tenzir --dump-ast`)
- `ir` - intermediate representation (`tenzir --dump-ir`)
- `finalize` - finalized pipeline (`tenzir --dump-finalized`)
- `instantiation` - diff between `--dump-ir` and `--dump-inst-ir`
- `opt` - diff between instantiated and optimized IR (`--dump-opt-ir`)
- `oldir` - legacy IR dumper (`--dump-pipeline`)

## Fixtures

The following project-specific fixtures exist in `test/fixtures/`:

- `http` - HTTP echo server (`HTTP_FIXTURE_URL`, `HTTP_FIXTURE_ENDPOINT`, `HTTP_CAPTURE_FILE`)
- `tcp_tls_source` - TLS TCP source (`TCP_TLS_ENDPOINT`, `TCP_TLS_CERTFILE`, `TCP_TLS_KEYFILE`, `TCP_TLS_CAFILE`)
- `tcp_sink` - TCP capture server (`TCP_SINK_ENDPOINT`, `TCP_SINK_FILE`)
- `udp_source` - UDP packet source (`UDP_SOURCE_ENDPOINT`)
- `udp_sink` - UDP capture server (`UDP_SINK_ENDPOINT`, `UDP_SINK_FILE`)

## Documentation

Primary documentation about the test framework:

https://docs.tenzir.com/reference/test-framework.md 

Use-case specific guides:

- https://docs.tenzir.com/guides/testing/write-tests.md
- https://docs.tenzir.com/guides/testing/create-fixtures.md
- https://docs.tenzir.com/guides/testing/add-custom-runners.md

