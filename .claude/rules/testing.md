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
- `tcp` - TCP client/server with optional TLS (`TCP_ENDPOINT`; with `tls: true`: `TCP_CERTFILE`, `TCP_KEYFILE`, `TCP_CAFILE`, `TCP_CERTKEYFILE`; with `mode: server`: `TCP_FILE`)
- `mysql` - MySQL container with optional TLS and streaming
  (`MYSQL_HOST`, `MYSQL_PORT`, ...; with `tls: true`: `MYSQL_TLS_*`;
  with `streaming: unsigned`: `MYSQL_LIVE_STREAM_TABLE`, `MYSQL_LIVE_STREAM_TOKEN`;
  with `streaming: signed`: `MYSQL_LIVE_SIGNED_STREAM_TABLE`, `MYSQL_LIVE_SIGNED_STREAM_TOKEN`)
- `udp_source` - UDP packet source (`UDP_SOURCE_ENDPOINT`)
- `udp_sink` - UDP capture server (`UDP_SINK_ENDPOINT`, `UDP_SINK_FILE`)

## Documentation

Primary documentation about the test framework:

https://docs.tenzir.com/reference/test-framework.md

Use-case specific guides:

- https://docs.tenzir.com/guides/testing/run-tests.md
- https://docs.tenzir.com/guides/testing/write-tests.md
- https://docs.tenzir.com/guides/testing/create-fixtures.md
- https://docs.tenzir.com/guides/testing/add-custom-runners.md
