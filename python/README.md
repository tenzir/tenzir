# Tenzir Python

The Python package of Tenzir provides a flexible control plane to integrate Tenzir
with other security tools.

> **Note**
> The Python effort is still highly experimental and subject to rapid change.
> Please do not consider it for production use.

## Usage

To get started, clone the Tenzir repository and install the Python package via
[uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/tenzir/tenzir.git
cd tenzir/python
uv sync --extra module
```

## Development

We recommend an editable installation, which `uv sync` creates by default.

### Unit Tests

Run the unit tests via pytest:

```bash
uv run pytest
```

### Integration Tests

Run the integrations tests via Docker Compose and pytest:

```bash
./docker-uv-run.sh pytest -v
```

## Packaging

The following instructions concern maintainers who want to publish the Python
package to PyPI.

> **Note**
> Our releasing scripts and CI run these steps automatically. You do not need to
> intervene anywhere. The instructions below merely document the steps taken.

### Bump the version

Prior to releasing a new version, bump the version, e.g.:

```bash
uv version 2.3.1
```

This updates the `pyproject.toml` file.

## Bundled CLI binaries (Linux)

For Linux wheels, you can bundle static `tenzir` and `tenzir-ctl` binaries and their resources built via Nix.

- Build static binaries: `nix build .#tenzir-static` (and/or `.#tenzir-de-static`).
- Copy resulting directories into the package:
  - `result/bin` → `python/tenzir/bin/`
  - `result/libexec` → `python/tenzir/libexec/` (if present)
  - `result/share` → `python/tenzir/share/` (if present)
- Build the wheel: `uv build`.

The wheel exposes `tenzir` and `tenzir-ctl` console scripts that prefer the bundled
static binaries when present, falling back to the system `PATH` otherwise. Non-Linux
platforms are supported via fallback only and are not expected to include binaries.

### Publish to Test PyPI

1. Get the token from <https://test.pypi.org/manage/account/token/>.

2. Publish:

   ```bash
   uv publish --publish-url https://test.pypi.org/legacy/ --check-url https://test.pypi.org/simple/ --token pypi-XXXXXXXX
   ```

### Publish to PyPI

1. Get the token from <https://pypi.org/manage/account/token/>.

2. Publish

   ```bash
   uv publish --token pypi-XXXXXXXX
   ```
