# Quack dependency probe (TNZ-1456)

This standalone C API probe validates Tenzir's bundled DuckDB 2.0 package and
its Quack and `httpfs` extensions. It uses the production DuckDB override and
`FindDuckDB.cmake` to exercise shared-library and static-archive consumers.

## Checks

The probe runs separate private in-memory client and server instances over
loopback. It checks:

- Embedded Quack and `httpfs` extensions with automatic installation disabled.
- Scoped authentication tokens, catalog attachment, and read-only transactions.
- Query and direct appenders, including selected columns and omitted defaults.
- Multiple data chunks with integers, Unicode, embedded NULs, and nulls.
- HTTP and verified TLS, including rejection of an untrusted certificate.
- SQL and appender rollbacks, including a write after a remote read.

Networking, authentication, TLS, data, and rollback failures return a nonzero
status.

## Build details

The package definition selects Tenzir's production DuckDB override, including
its pinned extension sources and networking dependencies. The find module
collects sibling extension archives, adds their external link dependencies,
and generates the registration code required by DuckDB 2.0 static consumers.

The probe matches the production connection settings by limiting DuckDB to one
thread and disabling the remote pushdown optimizer when it is available. Its
TLS checks enable certificate verification, reject an untrusted certificate,
and retry with the generated CA certificate.

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
or an external database service.

To evaluate the musl derivation, select
`f.legacyPackages.x86_64-linux.pkgsStatic` instead of the native package set.
Building and running it still requires a suitable Linux builder.

## Remaining validation

Shared-library and static-archive probes complement the final Tenzir build.
Run the actual static executable on each supported platform to verify the
complete link and runtime behavior. Repeat the probe when upgrading DuckDB or
Quack, using the exact client and server versions you intend to support.

## Sources

- [TNZ-1456](https://linear.app/tenzir/issue/TNZ-1456)
- [Quack overview](https://duckdb.org/docs/current/quack/overview)
