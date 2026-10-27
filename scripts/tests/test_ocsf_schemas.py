"""Check that bundled OCSF schemas can be resolved without recursive types.

Run with:
    uv run --no-project --with pytest pytest scripts/tests/test_ocsf_schemas.py
"""

from pathlib import Path
import re

import pytest


SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema/types/ocsf"


@pytest.mark.parametrize(
    "path", sorted(SCHEMA_DIR.glob("*.schema")), ids=lambda p: p.stem
)
def test_bundled_schema_has_no_recursive_or_undefined_types(path):
    definitions = dict(
        re.findall(r"^type (\S+) = record\{(.*?)^\}", path.read_text(), re.M | re.S)
    )
    assert definitions, f"No type definitions found in {path}"
    resolved = set()
    resolving = []

    def resolve(name):
        assert name in definitions, f"Undefined type: {name}"
        assert name not in resolving, "Recursive types: " + " -> ".join(
            [*resolving, name]
        )
        if name in resolved:
            return
        resolving.append(name)
        for target in re.findall(r"_ocsf\.[\w.]+", definitions[name]):
            resolve(target)
        resolving.pop()
        resolved.add(name)

    for name in definitions:
        resolve(name)
