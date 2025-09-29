# Bats → tenzir-test Migration Plan

## Objectives
- Replace the remaining Bats suites with tenzir-test scenarios while consolidating on TQL2 syntax.
- Preserve high-value coverage (schedulers, transports, storage formats, node lifecycle) during the migration.
- Retire legacy-only coverage or rewrite it against the modern language surface where the functionality is still relevant.

## Working Guidelines
- Rely on the built-in `node` fixture; request additional hooks only when we discover gaps during individual ports.
- Introduce a dedicated `pyarrow` fixture before migrating `feather.bats`.
- Provide HTTP and socket helpers in-house, following `example-project/fixtures/http.py` from the `tenzir-test` repo as a template.
- Decompose `pipelines_local.bats` so each behaviour lands in the appropriate directory under `test/tests/*`.
- Prefer inline `from { … }` data (with `write_lines` when needed) over `shell` commands to keep scenarios self-contained.
- Run `uvx tenzir-test` from the `test/` directory before staging changes so baselines capture stable relative paths.
- Remove the corresponding `tenzir/bats/data/reference/**` artefacts once a Bats suite is ported to tenzir-test.

## Test Suite Inventory & Target State
| File | Focus | Current State | Migration Decision |
| --- | --- | --- | --- |
| tests/cron.bats | Scheduler modifiers (`cron`, remote) | TQL2 | Ported to `tests/node/scheduler/*.tql` and `tests/operators/cron/*.tql`. |
| tests/every.bats | Scheduler modifiers (`every`, remote) | TQL2 | Ported to `tests/node/scheduler/*.tql` and `tests/operators/every/*.tql`. |
| tests/time.bats | Duration arithmetic & rounding | TQL2 | Ported to `tests/functions/time/*.tql`. |
| tests/functions.bats | TQL2 scalar & record functions | TQL2 | Ported to `tests/functions/**` scenarios. |
| tests/from_to.bats | URI auto-deduction | TQL2 | Ported to `tests/compiler/from_to/*.tql`. |
| tests/gelf.bats | `read_gelf`, `unflatten` | TQL2 | Ported to `tests/operators/read_gelf/**/*.tql`. |
| tests/lines.bats | `read_lines` null delimiters | TQL2 | Ported to `tests/operators/read_delimited/null_separator.tql`. |
| tests/database.bats | Node metrics & import/export | TQL2 | Ported to `tests/node/storage/*.tql`. |
| tests/feather.bats | Arrow/Feather roundtrip & compression | TQL2 + Python deps | Port after providing pyarrow/uv fixture in tenzir-test or stubbing via golden files. |
| tests/http2.bats, tests/opensearch.bats | HTTP load/save & OpenSearch bulk | TQL2 | Port with HTTP mock server support; prefer built-in runner over ad-hoc `python webserver.py`. |
| tests/tcp.bats, tests/udp.bats | Socket transports (TLS/UDP) | TQL2 | Port once tenzir-test exposes socket helpers & certificate fixtures; keep TLS coverage. |
| tests/shutdown.bats | Remote operator shutdown | TQL2 | Retired; behaviour covered by node lifecycle tests elsewhere. |
| tests/version.bats | `version` smoke | TQL2 | Retire; existing CLI smoke coverage is sufficient. |
| tests/pipelines_local.bats | Mixed parser coverage, heavy `TENZIR_LEGACY` | TQL1 | Triage: rewrite relevant behaviours in TQL2 (schema selectors, charts, deduplicate, unroll); drop legacy-only parser assertions. |
| tests/leef.bats | `read_leef`, `parse_leef` under legacy flag | TQL1 | Ported to `tests/operators/leef/*.tql` with modern syntax. |
| tests/vast.bats, tests/vast_server.bats | Old node pipelines & filters | TQL1 | Identify behaviours still missing in TQL2 suites (import/export filtering, schema predicates) and rewrite; remove redundant legacy syntax checks. |
| lib/** | Vendored Bats framework self-tests | N/A | Drop after migration (not part of Tenzir coverage). |

## Capability Prerequisites for tenzir-test
- **Node orchestration:** replicate `setup_node_with_default_config` / `setup_node` fixtures (state directory, config overrides, cleanup).
- **Data directories:** expose `${INPUTSDIR}`/`${MISCDIR}` equivalents for fixtures referenced by existing tests.
- **Process helpers:** replace `check --bg` patterns with tenzir-test async steps; add wrappers for HTTP mock servers, socat/openssl, UDP/TCP clients.
- **Python dependencies:** pre-install or vendor `pyarrow`, `trustme`, and small helper scripts (`print-arrow-batch-size.py`). Consider snapshotting expected outputs to avoid external installs.
- **Result assertions:** tenzir-test should offer helpers for stdout/stderr captures, exit code checks (success/failure), and warning normalization (e.g., truncation message regex in feather.bats).

## TQL1 → TQL2 Translation Guide
| Legacy idiom | TQL2 replacement | Reference |
| --- | --- | --- |
| `from file PATH read json` | `from "PATH" { read_json }` | [`read_json`](https://docs.tenzir.com/reference/operators/read_json), [`from`](https://docs.tenzir.com/reference/operators/from) |
| `load file PATH | read json` | `load_file "PATH" { read_json }` | [`load_file`](https://docs.tenzir.com/reference/operators/load_file) |
| `write json`, `write csv`, … | `write_json`, `write_csv`, … (underscored operator names) | [`write_json`](https://docs.tenzir.com/reference/operators/write_json) |
| `print field json` | `field.print_json()` followed by `write_json` or assignments | [`print_json`](https://docs.tenzir.com/reference/functions/print_json) |
| `parse field json/grok/leef` | `field.parse_json()`, `field.parse_grok()`, `field.parse_leef()` | [`parse_json`](https://docs.tenzir.com/reference/functions/parse_json), [`parse_grok`](https://docs.tenzir.com/reference/functions/parse_grok), [`parse_leef`](https://docs.tenzir.com/reference/functions/parse_leef) |
| `put foo = expr` / `extend` / `replace` | Direct assignments (`foo = expr`) or record literals (`this = { … }`); implicitly uses [`set`](https://docs.tenzir.com/reference/operators/set) | [Assignments](https://docs.tenzir.com/explanations/language/statements/#assignment) |
| `flatten` / `unflatten` operators | `this = flatten(this)` / `this = unflatten(this)` or field-scoped variants | [`flatten`](https://docs.tenzir.com/reference/functions/flatten), [`unflatten`](https://docs.tenzir.com/reference/functions/unflatten) |
| `chart pie/bar/line …` | `chart_pie`, `chart_bar`, `chart_line` operators with explicit options | [`chart_pie`](https://docs.tenzir.com/reference/operators/chart_pie) et al. |
| `read cef`, `read suricata`, … | `read_cef`, `read_suricata`, … (underscored operator names) | Operator index |
| `parse content leef` | `content = content.parse_leef()` | [`parse_leef`](https://docs.tenzir.com/reference/functions/parse_leef) |
| `yield path` | Prefer `unroll` / `select` with list access (`unroll dns.answers`) or explicit list comprehensions | [`unroll`](https://docs.tenzir.com/reference/operators/unroll), [Programs](https://docs.tenzir.com/explanations/language/programs) |
| `to stdout write json` | `write_json | save_stdout` or rely on default stdout sink | [`write_json`](https://docs.tenzir.com/reference/operators/write_json), [`save_stdout`](https://docs.tenzir.com/reference/operators/save_stdout) |
| `set-attributes`, `get-attributes` | Use structured assignments on `attributes` or dedicated helper operators (confirm against current operator docs) | Pending doc lookup |

> **Action:** Validate the remaining unknowns (`set-attributes`, `yield` refactors) against current operator docs and update this table before implementation.

## Migration Phases
1. **Foundation (Week 1)**
   - Implement tenzir-test fixtures for node lifecycle, I/O helpers, and env vars.
   - Confirm TQL2 replacements for outstanding legacy operators (`set-attributes`, `yield`).
   - Mirror essential helper scripts (HTTP server, Arrow inspector) or replace with Rust/Python-less alternatives.
2. **Quick wins (Week 2)**
   - Port pure TQL2 suites (cron, every, time, functions, from_to, gelf, lines, version).
   - Add regression coverage for `measure`, `deduplicate`, `unroll` using TQL2 syntax while porting.
3. **Integration-heavy ports (Week 3)**
   - Migrate database, feather, http2/opensearch, tcp/udp, shutdown after fixtures mature.
   - Introduce reusable tenzir-test modules for HTTP mock servers, TLS cert generation, and UDP echo clients.
4. **Legacy rewrites (Week 4+)**
   - For pipelines_local/leef/vast/vast_server, catalogue behaviours worth keeping (e.g., schema selectors, import filters, chart validation) and re-author them in TQL2.
   - Drop tests that only assert legacy parser quirks (`--dump-ast`, `load file` syntax errors, etc.).
5. **Cleanup**
   - Remove vendored `lib/bats` subtree and obsolete helper scripts.
   - Document the migration in CHANGELOG (tests / infrastructure) and update developer docs.

## Open Questions / Risks
- Ensure tenzir-test can depend on external binaries (`socat`, `openssl`, `lsof`) or provide Rust-native substitutes.
- Decide whether to keep dynamic dependency installation (pyarrow, trustme) or rely on prebuilt fixtures.
- Revisit warning normalization (e.g., Arrow truncation message) to make assertions stable across dependency versions.
- Confirm availability of `set_attributes` / `get_attributes` equivalents; adjust plan if operator semantics changed.

## References
- Operators catalog: https://docs.tenzir.com/reference/operators.md
- Functions catalog: https://docs.tenzir.com/reference/functions.md
- TQL language overview:
  - Expressions: https://docs.tenzir.com/explanations/language/expressions.md
  - Statements: https://docs.tenzir.com/explanations/language/statements.md
  - Programs: https://docs.tenzir.com/explanations/language/programs.md
