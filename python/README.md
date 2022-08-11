# VAST Python

VAST comes with a Python package for easy management and integration.

## Usage

Setup a virtual environment:

```bash
python3 -m venv venv
. venv/bin/activate
pip install -e .
```

Thereafter, you should be able to run files in the `examples` directory, e.g.:

```bash
python examples/misp.py
```

## Unit Tests

Run all unit tests as follows:

```bash
pytest
```
