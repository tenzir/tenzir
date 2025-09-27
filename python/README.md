# Tenzir Python

The Python package of Tenzir provides a flexible control plane to integrate Tenzir
with other security tools.

> **Note**
> The Python effort is still highly experimental and subject to rapid change.
> Please do not consider it for production use.

## Packages

This workspace builds three distributions:

- `tenzir-common`: Shared runtime utilities and configuration helpers used across
  the Python ecosystem.
- `tenzir-operators`: The Python operator executor that powers the `python`
  pipeline stage when running inside Tenzir.
- `tenzir`: The public CLI bindings, including compatibility shims for the
  legacy `tenzir` import path and optional bundled binaries.

## Usage

To get started, clone the Tenzir repository and install the CLI bindings via
[uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/tenzir/tenzir.git
cd tenzir/python
uv sync --package tenzir --extra module
```

Add `--extra operator` if you need the Python operator helpers
(`tenzir-operators`).

## Development

We recommend an editable installation. To work on all workspace members with the
shared development dependencies run:

```bash
uv sync --all-packages --group dev
```

### Unit Tests

Run the unit tests via pytest:

```bash
uv run --all-packages pytest
```

### Integration Tests

Run the integrations tests via Docker Compose and pytest:

```bash
./docker-uv-run.sh --all-packages pytest -v
```

## Packaging

The following instructions concern maintainers who want to publish the Python
packages to PyPI.

> **Note**
> Our releasing scripts and CI run these steps automatically. You do not need to
> intervene anywhere. The instructions below merely document the steps taken.

### Bump the version

Update each member package individually, for example:

```bash
uv version --package tenzir-core 2.3.1
uv version --package tenzir-operators 2.3.1
uv version --package tenzir 2.3.1
```

### Build distributions

Create wheels and sdists for the packages that changed:

```bash
uv build --package tenzir-core
uv build --package tenzir-operators
uv build --package tenzir
```

## Bundled CLI binaries (Linux)

For Linux wheels, you can bundle static `tenzir` and `tenzir-ctl` binaries and their resources built via Nix.

- Build static binaries: `nix build .#tenzir-static` (and/or `.#tenzir-de-static`).
- Copy resulting directories into the CLI package:
  - `result/bin` → `python/tenzir/src/tenzir/bin/`
  - `result/libexec` → `python/tenzir/src/tenzir/libexec/` (if present)
  - `result/share` → `python/tenzir/src/tenzir/share/` (if present)
- Build the wheel: `uv build --package tenzir`.

The wheel exposes `tenzir` and `tenzir-ctl` console scripts that prefer the bundled
static binaries when present, falling back to the system `PATH` otherwise. Non-Linux
platforms are supported via fallback only and are not expected to include binaries.

### Publish to Test PyPI

1. Get the token from <https://test.pypi.org/manage/account/token/>.
2. Publish the desired package(s), e.g. the CLI wheel:

   ```bash
   uv publish --publish-url https://test.pypi.org/legacy/ --check-url https://test.pypi.org/simple/ --token pypi-XXXXXXXX dist/tenzir-*.whl dist/tenzir-*.tar.gz
   ```

### Publish to PyPI

1. Get the token from <https://pypi.org/manage/account/token/>.
2. Publish the desired package(s):

   ```bash
   uv publish --token pypi-XXXXXXXX dist/tenzir_core-*.whl dist/tenzir_core-*.tar.gz
   uv publish --token pypi-XXXXXXXX dist/tenzir_operators-*.whl dist/tenzir_operators-*.tar.gz
   uv publish --token pypi-XXXXXXXX dist/tenzir-*.whl dist/tenzir-*.tar.gz
   ```
