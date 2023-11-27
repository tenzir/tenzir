# Tenzir Python

The Python package of Tenzir provides a flexible control plane to integrate Tenzir
with other security tools.

> **Note**
> The Python effort is still highly experimental and subject to rapid change.
> Please do not consider it for production use.

## Usage

To get started, clone the Tenzir repository and install the Python package via
[Poetry](https://python-poetry.org/docs/):

```bash
git clone https://github.com/tenzir/tenzir.git
cd tenzir/python
poetry install -E module
```

## Development

We recommend that you work with an editable installation, which is the default
for `poetry install`.

### Unit Tests

Run the unit tests via pytest:

```bash
poetry run pytest
```

### Integration Tests

Run the integrations tests via Docker Compose and pytest:

```bash
./docker-poetry-run.sh pytest -v
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
poetry version 2.3.1
```

This updates the `pyproject.toml` file.

### Publish to Test PyPI

1. Add a Test PyPi repository:

   ```bash
   poetry config repositories.test-pypi https://test.pypi.org/legacy/
   ```

2. Get the token from <https://test.pypi.org/manage/account/token/>.

3. Store the token:

  ```bash
  poetry config pypi-token.test-pypi pypi-XXXXXXXX
  ```

4. Publish:
  
   ```bash
   poetry publish --build -r test-pypi
   ```

### Publish to PyPI

1. Get the token from <https://pypi.org/manage/account/token/>.

2. Store the token:

  ```bash
  poetry config pypi-token.pypi pypi-XXXXXXXX
  ```

3. Publish

   ```bash
   poetry publish --build
   ```
