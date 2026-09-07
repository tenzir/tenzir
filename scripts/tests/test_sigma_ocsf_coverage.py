"""Tests for scripts/sigma-ocsf-coverage.py.

Run with:
    uv run --no-project --with pytest --with pyyaml pytest scripts/tests
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

pytest.importorskip("yaml")

SCRIPT = Path(__file__).resolve().parents[1] / "sigma-ocsf-coverage.py"


def load_script():
    spec = importlib.util.spec_from_file_location("sigma_ocsf_coverage", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


coverage = load_script()


def test_rule_fields_names_the_key_before_its_modifiers():
    detection = {
        "selection": {
            "Image|endswith": "\\cmd.exe",
            "CommandLine|contains|all": ["a", "b"],
        },
        "condition": "selection",
    }
    assert coverage.rule_fields(detection) == {"Image", "CommandLine"}


def test_rule_fields_includes_scalar_fieldref_targets():
    detection = {
        "selection": {"Image|fieldref": "ParentImage"},
        "condition": "selection",
    }
    assert coverage.rule_fields(detection) == {"Image", "ParentImage"}


def test_rule_fields_includes_every_string_in_a_fieldref_list():
    detection = {
        "selection": {"Image|fieldref": ["ParentImage", "OriginalFileName", 7]},
        "condition": "selection",
    }
    # Like the engine planner, only string values name fields.
    assert coverage.rule_fields(detection) == {
        "Image",
        "ParentImage",
        "OriginalFileName",
    }


def test_rule_fields_finds_fieldref_anywhere_in_the_modifier_chain():
    detection = {
        "selection": [{"Image|fieldref|endswith": "ParentImage"}],
        "condition": "selection",
    }
    assert coverage.rule_fields(detection) == {"Image", "ParentImage"}


def test_rule_fields_ignores_constant_values_and_keywords():
    detection = {
        "selection": {"Image|contains": "ParentImage"},
        "keywords": ["ParentImage"],
        "condition": "selection and keywords",
        "timeframe": "5m",
    }
    assert coverage.rule_fields(detection) == {"Image"}


def write_rule(path: Path, title: str) -> None:
    path.write_text(
        f"title: {title}\nlogsource:\n  product: windows\ndetection:\n"
        "  selection:\n    Image: x\n  condition: selection\n"
    )


def test_load_rules_reads_yml_and_yaml_once_each(tmp_path: Path):
    write_rule(tmp_path / "a.yml", "A")
    write_rule(tmp_path / "b.yaml", "B")
    nested = tmp_path / "nested"
    nested.mkdir()
    write_rule(nested / "c.yaml", "C")
    write_rule(nested / "d.yml", "D")
    (tmp_path / "ignored.txt").write_text("title: not a rule\n")
    (tmp_path / "README.md").write_text("# not a rule\n")
    rules = coverage.load_rules(tmp_path)
    titles = sorted(rule["title"] for _, rule in rules)
    assert titles == ["A", "B", "C", "D"]
    assert len({path for path, _ in rules}) == 4
    assert [path for path, _ in rules] == sorted(path for path, _ in rules)


def test_load_rules_counts_every_document_of_a_multi_document_file(tmp_path: Path):
    (tmp_path / "multi.yaml").write_text(
        "title: One\nlogsource: {product: windows}\n"
        "---\n"
        "title: Two\nlogsource: {product: windows}\n"
        "---\n"
        "just: metadata\n"
    )
    rules = coverage.load_rules(tmp_path)
    assert [rule["title"] for _, rule in rules] == ["One", "Two"]
