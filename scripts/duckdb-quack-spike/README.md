# Quack bundling spike (TNZ-1456)

## Recommendation

**Bundling works; remote rollback remains a documented beta limitation.**

Quack and its required `httpfs` extension build into DuckDB with one header-path
patch, pinned source downloads, and the OpenSSL/curl dependencies Tenzir already
uses. Both the shared library and static DuckDB archives work over loopback HTTP
and verified TLS on Apple Silicon; the transaction checks fail in both cases.

Writes into an existing remote table survive `ROLLBACK`, with ordinary SQL and
both appenders. TNZ-1456 intentionally does not require transaction atomicity:
remote writes can ship with this limitation documented, without waiting for
DuckDB 2.0. The probe reports rollback failures without failing the networking
and data checks.

The production override and operators now include Quack support. This directory
retains the standalone C API probe for dependency upgrades and static-link
validation, using the production DuckDB package and find module.

## Tested versions

Both client and server use the same DuckDB library and the extension commits
selected by that release's upstream build configuration:

| Component | Version or commit |
| --- | --- |
| DuckDB | `1.5.5` (`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`) |
| Quack | `c1548111c1bfd16207e22fd3cb7e4bde1335b9d0` |
| httpfs | `827222fb45a043a7a852d1f7aae46901492a3cda` |
| OpenSSL | `3.6.3` |
| curl | `8.21.0`, Tenzir's `curl-ws` variant |

This isn't a cross-version compatibility guarantee. Quack is a beta protocol;
repeat these tests for the exact client/server release pair before documenting
support.

## Results

| Check | Result |
| --- | --- |
| Nix build on `aarch64-darwin` | Pass |
| Quack and httpfs embedded in `libduckdb.dylib` | Pass |
| Link the same probe against `libduckdb_static.a` and extension archives | Pass, after adding curl/OpenSSL link dependencies |
| Build `pkgsStatic.duckdb` and run the probe with static curl/OpenSSL on macOS | Pass, using pkg-config for curl's transitive dependencies |
| Empty home directory, automatic extension installation disabled | Pass; both extensions report `STATICALLY_LINKED` |
| Scoped token secret, `ATTACH`, and `USE` | Pass |
| Query appender with three data chunks (4,113 rows) | Pass |
| Integers, Unicode, embedded NULs, nulls, and omitted constant defaults | Pass |
| Direct appender with a selected column list | Pass |
| Read a remote table in a read-only transaction | Pass |
| TLS through a local reverse proxy | Pass, including rejection of an untrusted certificate |
| Roll back writes to an existing table | **Fail** for SQL and both appenders |
| Start a remote read before the query-appender write, then roll back | **Fail** |
| Evaluate the Linux musl package derivation | Pass |
| Build/run a fully static Linux binary | **Not tested**; no Linux builder was configured |

The original macOS static-archive probe dynamically linked curl and OpenSSL.
The subsequent `pkgsStatic` build and probe also link these networking libraries
statically, leaving only Apple system libraries and frameworks dynamic. Neither
proves that the final musl-linked Tenzir executable works. The implementation
was also tested with the full macOS Tenzir binary and fixture-backed integration
tests.

The two additional extension archives are approximately 1.7 MiB (Quack) and
1.9 MiB (httpfs) in this build. These are archive sizes, not a measurement of the
final executable's size increase.

### Transaction failure

The probe uses separate private in-memory client/server instances, communicating
over loopback. The server creates the table before the client attaches. A simple
client-side transaction already reproduces the problem:

```sql
BEGIN;
INSERT INTO remote.main.events (id) VALUES (-1);
ROLLBACK;
```

An independent query on the server still sees the inserted row. The data-chunk
probe produces the following observations over both HTTP and HTTPS:

```text
SQL INSERT rollback: expected 0 rows, got 1
query-appender rollback: expected 4113 rows, got 8226
PASS: direct appender with column selection and defaults
direct-appender rollback: expected 8226 rows, got 12339
query-appender rollback after remote SELECT: expected 8226 rows, got 12339
```

A likely upstream cause is that the existing-table path in
`QuackInsert::GetGlobalSinkState` doesn't start the remote transaction, while the
create-table path does. `QuackInsert::Sink` sends append requests directly. This
is a source-inspection hypothesis, not a validated fix; this spike doesn't patch
transaction semantics.

### The direct appender works

The issue's assumption that `duckdb_appender_create_ext` bypasses SQL planning
doesn't hold for this version. DuckDB's `Appender::FlushInternal` constructs an
`INSERT` statement. The probe successfully writes through the remote attachment
with `duckdb_appender_add_column` and preserves an omitted constant default.
There is no demonstrated need to replace it with the query appender.

A query-appender replacement would also need to preserve the existing sink's
column-selection behavior: DuckDB's query appender doesn't support `AddColumn`
or `ClearColumns`. The operator integration tests cover constant defaults,
generated columns, constraints, and create/append modes. In this Quack version,
sequence-backed defaults prevent catalog attachment, including writes to other
tables in that database. Readers avoid this restriction by querying the server
directly rather than attaching its catalog.

### TLS must be configured explicitly

The pinned httpfs extension defaults `enable_server_cert_verification` to
`false` for its httplib backend. The probe explicitly enables it, verifies that
an untrusted certificate fails, then supplies the generated CA certificate and
repeats the read/write tests over HTTPS. Production support must not rely on the
default. The curl backend has a separate verification setting, enabled by
default.

## Build details

`duckdb.nix` now selects Tenzir's production DuckDB override. Static builds use
`core_functions`, `json`, `parquet`, `httpfs`, and `quack`, retaining the ICU
exclusion. Shared builds keep DuckDB's in-tree extension set and add networking.
The original measurements used a minimal shared extension set and do not
measure the size of the production shared build.

Quack's pinned CMake file assumes a nested DuckDB checkout. The override rewrites
two include paths to use the enclosing DuckDB source. It doesn't fetch the
extension's DuckDB or CI-tool submodules, use VCPKG, or download extensions at
runtime. Both extension source archives have fixed hashes.

`CMakeLists.txt` uses the production `FindDuckDB.cmake`, which collects sibling
extension archives and adds curl/OpenSSL dependencies for static consumers.

## Reproduce

Run from `engine/` in the development shell (`nix develop`). This builds only the
production DuckDB package and a standalone C API probe, not Tenzir:

```sh
repo=$(git rev-parse --show-toplevel)
system=$(nix eval --impure --raw --expr builtins.currentSystem)
package="let f = builtins.getFlake \"git+file://$repo?dir=engine\";
  in import $PWD/scripts/duckdb-quack-spike/duckdb.nix {
    pkgs = f.legacyPackages.$system;
  }"
nix build --impure --expr "$package" --out-link build/quack-spike/duckdb

dev=$(nix eval --impure --raw --expr "($package).dev.outPath")
lib=$(nix eval --impure --raw --expr "($package).lib.outPath")
case $(uname -s) in
  Darwin) shared_suffix=dylib ;;
  Linux) shared_suffix=so ;;
esac

cmake -S scripts/duckdb-quack-spike -B build/quack-spike/probe-shared \
  -DDuckDB_INCLUDE_DIR="$dev/include" \
  -DDuckDB_LIBRARY="$lib/lib/libduckdb.$shared_suffix"
cmake --build build/quack-spike/probe-shared
python3 scripts/duckdb-quack-spike/run.py \
  build/quack-spike/probe-shared/quack-probe

cmake -S scripts/duckdb-quack-spike -B build/quack-spike/probe-static \
  -DTENZIR_ENABLE_STATIC_EXECUTABLE=ON \
  -DDuckDB_INCLUDE_DIR="$dev/include" \
  -DDuckDB_LIBRARY="$lib/lib/libduckdb_static.a"
cmake --build build/quack-spike/probe-static
python3 scripts/duckdb-quack-spike/run.py \
  build/quack-spike/probe-static/quack-probe
```

To check the static networking dependencies as well, build the `pkgsStatic`
package and configure the probe in its isolated build environment. This avoids
accidentally finding shared curl or OpenSSL from the surrounding development
shell:

```sh
package=".#legacyPackages.$system.pkgsStatic.duckdb"
nix build "$package" --out-link build/quack-spike/duckdb-pkgs-static
dev=$(nix eval --raw "$package.dev.outPath")
lib=$(nix eval --raw "$package.lib.outPath")
nix develop --ignore-environment --keep HOME "$package" -c \
  cmake -S scripts/duckdb-quack-spike -B build/quack-spike/probe-pkgs-static \
    -DTENZIR_ENABLE_STATIC_EXECUTABLE=ON -DOPENSSL_USE_STATIC_LIBS=TRUE \
    -DDuckDB_INCLUDE_DIR="$dev/include" \
    -DDuckDB_LIBRARY="$lib/lib/libduckdb_static.a"
nix develop --ignore-environment --keep HOME "$package" -c \
  cmake --build build/quack-spike/probe-pkgs-static
python3 scripts/duckdb-quack-spike/run.py \
  build/quack-spike/probe-pkgs-static/quack-probe
```

The runner uses Python's standard library and the `openssl` executable. It
creates disposable certificates and a home directory, binds only to loopback,
and stops its proxy even when a probe fails. It doesn't use ambient credentials
or an external database service. Networking, authentication, TLS, and data
failures return a nonzero status. Rollback failures are informational and do
not change the exit status.

To evaluate the musl derivation, select
`f.legacyPackages.x86_64-linux.pkgsStatic` instead of the native package set.
Building and running it still requires a suitable Linux builder.

## Remaining validation

Build and run the actual Linux static executable on a suitable builder; the
macOS archive probe does not replace this check. Retest transaction behavior
and version compatibility when upgrading DuckDB or Quack. Do not assume local
batch atomicity for remote writes.

## Sources

- [TNZ-1456](https://linear.app/tenzir/issue/TNZ-1456)
- [Quack overview and beta warning](https://duckdb.org/docs/current/quack/overview)
- [DuckDB 1.5.5 Quack pin](https://github.com/duckdb/duckdb/blob/v1.5.5/.github/config/extensions/quack.cmake)
- [DuckDB 1.5.5 httpfs pin](https://github.com/duckdb/duckdb/blob/v1.5.5/.github/config/extensions/httpfs.cmake)
- [Pinned Quack client and httpfs requirement](https://github.com/duckdb/duckdb-quack/blob/c1548111c1bfd16207e22fd3cb7e4bde1335b9d0/src/quack_client.cpp)
- [Pinned Quack insert path](https://github.com/duckdb/duckdb-quack/blob/c1548111c1bfd16207e22fd3cb7e4bde1335b9d0/src/storage/quack_insert.cpp)
- [DuckDB 1.5.5 appender implementation](https://github.com/duckdb/duckdb/blob/v1.5.5/src/main/appender.cpp)
