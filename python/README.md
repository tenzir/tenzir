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

We recommend that you work with an editable installation, which is the default
for `uv sync`.

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
uv version  # Check uv version
# Update version in pyproject.toml manually to 2.3.1
```

This updates the `pyproject.toml` file.

### Publish to Test PyPI

1. Add a Test PyPi repository:

   ```bash
   # Configure test PyPI in uv (if needed)
   export UV_INDEX_URL=https://test.pypi.org/simple/
   ```

2. Get the token from <https://test.pypi.org/manage/account/token/>.

3. Store the token:

  ```bash
  export UV_PUBLISH_TOKEN=pypi-XXXXXXXX
  ```

4. Publish:
  
   ```bash
   uv build
   uv publish --index-url https://test.pypi.org/legacy/ dist/*
   ```

### Publish to PyPI

1. Get the token from <https://pypi.org/manage/account/token/>.

2. Store the token:

  ```bash
  export UV_PUBLISH_TOKEN=pypi-XXXXXXXX
  ```

3. Publish

   ```bash
   uv build
   uv publish dist/*
   ```
