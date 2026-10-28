"""Check that bundled OCSF schemas can be resolved without recursive types.

Run with:
    uv run --no-project --with pytest --with requests pytest scripts/tests/test_ocsf_schemas.py
"""

import importlib.util
import io
from pathlib import Path
import re

import pytest


SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema/types/ocsf"


def type_definitions(text):
    return dict(re.findall(r"^type (\S+) = record\{(.*?)^\}", text, re.M | re.S))


def bundled_finding_schemas(directory):
    return [
        path
        for path in sorted(directory.glob("*.schema"))
        if tuple(int(part) for part in re.findall(r"\d+", path.stem)[:3]) >= (1, 10, 0)
    ]


def test_finding_schema_discovery_after_release(tmp_path):
    for name in ("v1_9_0", "v1_10_0", "v1_11_0_dev"):
        (tmp_path / f"{name}.schema").touch()
    assert [path.stem for path in bundled_finding_schemas(tmp_path)] == [
        "v1_10_0",
        "v1_11_0_dev",
    ]


@pytest.fixture
def generator():
    script = Path(__file__).resolve().parents[1] / "ocsf-schemas.py"
    spec = importlib.util.spec_from_file_location("ocsf_schemas", script)
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    return generator


@pytest.mark.parametrize(
    ("version", "enabled"),
    [
        ("1.0.0", False),
        ("1.9.0", False),
        ("1.10.0-dev", True),
        ("1.10.0", True),
        ("1.11.0-dev", True),
        ("2.0.0-dev", True),
    ],
)
@pytest.mark.parametrize("entity", ["finding_info", "related_event"])
def test_finding_recursion_starts_with_1_10(generator, version, enabled, entity):
    schema = {"version": version}
    assert bool(generator.recursive_fields_for(schema, entity)) == enabled
    assert generator.recursive_fields_for(schema, "process") == {
        "parent_process": "process"
    }


@pytest.fixture(
    params=[*bundled_finding_schemas(SCHEMA_DIR), "1.10.0-dev", "1.10.0", "1.11.0-dev"],
    ids=lambda value: value.stem if isinstance(value, Path) else f"generated-{value}",
)
def finding_definitions(request, generator):
    if isinstance(request.param, Path):
        path = request.param
        return f"_ocsf.{path.stem}.object", type_definitions(path.read_text())

    schema = {
        "version": request.param,
        "types": {"string_t": "string"},
        "objects": {
            "finding_info": {
                "name": "finding_info",
                "description": "Finding metadata.",
                "attributes": {
                    "uid": {"type": "string_t", "requirement": "required"},
                    "title": {"type": "string_t", "requirement": "recommended"},
                    "related_events": {
                        "object_type": "related_event",
                        "is_array": True,
                        "requirement": "optional",
                    },
                },
            },
            "related_event": {
                "name": "related_event",
                "description": "A related event or finding.",
                "attributes": {
                    "uid": {"type": "string_t", "requirement": "required"},
                    "finding_info": {
                        "object_type": "finding_info",
                        "requirement": "recommended",
                    },
                },
            },
        },
    }
    output = io.StringIO()
    generator.emit_objects(generator.Writer(output), schema)
    return generator.object_prefix(schema), type_definitions(output.getvalue())


@pytest.mark.parametrize(
    ("root", "nested_path"),
    [
        ("finding_info", "related_events.finding_info"),
        ("related_event", "finding_info.related_events.finding_info"),
    ],
)
def test_related_findings_preserve_nested_metadata(
    finding_definitions, root, nested_path
):
    prefix, definitions = finding_definitions
    name = f"{prefix}.{root}"
    for field in nested_path.split("."):
        fields = dict(re.findall(r"^  (\w+): (.+)", definitions[name], re.M))
        assert field in fields, f"Missing {field} in {name}"
        name = re.search(r"_ocsf\.[\w.]+", fields[field])[0]
    nested_fields = dict(re.findall(r"^  (\w+): (.+)", definitions[name], re.M))
    assert nested_fields["uid"] == "string #required,"
    assert nested_fields["title"] == "string #recommended,"
    assert "related_events" not in nested_fields


@pytest.mark.parametrize(
    "path", sorted(SCHEMA_DIR.glob("*.schema")), ids=lambda p: p.stem
)
def test_bundled_schema_has_no_recursive_or_undefined_types(path):
    definitions = type_definitions(path.read_text())
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
