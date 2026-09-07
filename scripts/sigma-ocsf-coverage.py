#!/usr/bin/env python3
"""Measures how much of a SigmaHQ rule corpus the OCSF mapping catalog covers.

The script reads every catalog document, reproduces the logsource selector
that the engine applies, and classifies each detection field of every rule as
projected, declared unmapped, or unresolved. A rule counts as covered when at
least one mapping alternative projects every field it uses; the engine skips
the rule for OCSF input otherwise.

Usage:
    scripts/sigma-ocsf-coverage.py <sigma-rules-dir> [--top N] [--json]
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    sys.exit("error: PyYAML is required (pip install pyyaml)")

CATALOG_DIR = (
    Path(__file__).resolve().parent.parent
    / "libtenzir/builtins/operators/sigma/catalog"
)


def load_catalog(directory: Path) -> list[dict]:
    documents = []
    for path in sorted(directory.rglob("*.yaml")):
        with path.open() as handle:
            document = yaml.safe_load(handle)
        document["_path"] = path
        documents.append(document)
    if not documents:
        sys.exit(f"error: no catalog documents in {directory}")
    return documents


def rule_fields(detection: dict) -> set[str]:
    """Collects every field a detection references, as the engine planner does.

    A key names its field before the first modifier. A `fieldref` item also
    references the field(s) its value names, so `Image|fieldref: ParentImage`
    yields both `Image` and `ParentImage`: the engine resolves both sides
    against the schema and skips the rule unless both project. Values are
    otherwise constants and never name fields.
    """
    fields: set[str] = set()

    def walk(node) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                name, _, modifiers = key.partition("|")
                fields.add(name)
                if "fieldref" not in modifiers.split("|"):
                    continue
                values = value if isinstance(value, list) else [value]
                fields.update(item for item in values if isinstance(item, str))
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for name, value in detection.items():
        if name in ("condition", "timeframe"):
            continue
        walk(value)
    return fields


def selector_matches(logsource: dict, selector: dict) -> bool:
    category = selector.get("category")
    product = selector.get("product")
    service = selector.get("service")
    optional_service = selector.get("optional-service", False)
    if category is None:
        if logsource.get("category") is not None:
            return False
    elif logsource.get("category") != category:
        return False
    if logsource.get("product") != product:
        return False
    rule_service = logsource.get("service")
    if optional_service:
        return rule_service is None or rule_service == service
    if service is None:
        return rule_service is None
    return rule_service == service


def load_rules(directory: Path) -> list[tuple[Path, dict]]:
    """Loads every YAML document with a logsource from `.yml` and `.yaml` files.

    SigmaHQ uses `.yml`, but derived corpora often use `.yaml`. The suffix
    globs are disjoint, and the set makes that explicit.
    """
    rules = []
    paths = {path for suffix in ("*.yml", "*.yaml") for path in directory.rglob(suffix)}
    for path in sorted(paths):
        with path.open() as handle:
            try:
                documents = list(yaml.safe_load_all(handle))
            except yaml.YAMLError as error:
                print(f"warning: {path}: {error}", file=sys.stderr)
                continue
        for document in documents:
            if isinstance(document, dict) and "logsource" in document:
                rules.append((path, document))
    return rules


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("rules", type=Path, help="SigmaHQ rules directory")
    parser.add_argument(
        "--catalog", type=Path, default=CATALOG_DIR, help="catalog directory"
    )
    parser.add_argument(
        "--top", type=int, default=15, help="unclaimed logsources to list"
    )
    parser.add_argument("--json", action="store_true", help="emit JSON")
    args = parser.parse_args()

    catalog = load_catalog(args.catalog)
    rules = load_rules(args.rules)

    families: dict[str, dict] = {}
    for document in catalog:
        selector = document["logsource"]
        key = (
            selector.get("category"),
            selector.get("product"),
            selector.get("service"),
            selector.get("optional-service", False),
        )
        family = families.setdefault(
            repr(key),
            {
                "selector": {
                    "category": key[0],
                    "product": key[1],
                    "service": key[2],
                    "optional_service": key[3],
                },
                "documents": [],
                "claimed": 0,
                "covered": 0,
                "keyword_only": 0,
                "unresolved": collections.Counter(),
                "unmapped": collections.Counter(),
            },
        )
        family["documents"].append(document)

    unclaimed = collections.Counter()
    covered_total = 0
    claimed_total = 0
    for _, rule in rules:
        logsource = rule.get("logsource", {})
        fields = rule_fields(rule.get("detection", {}))
        claiming = [
            family
            for family in families.values()
            if any(
                selector_matches(logsource, document["logsource"])
                for document in family["documents"]
            )
        ]
        if not claiming:
            unclaimed[
                (
                    logsource.get("product"),
                    logsource.get("category"),
                    logsource.get("service"),
                )
            ] += 1
            continue
        family = claiming[0]
        family["claimed"] += 1
        claimed_total += 1
        if not fields:
            family["keyword_only"] += 1
            family["covered"] += 1
            covered_total += 1
            continue
        best_missing: set[str] | None = None
        best_unmapped: set[str] = set()
        for document in family["documents"]:
            projected = set(document.get("fields", {}))
            unmapped = {entry["field"] for entry in document.get("unmapped", [])}
            missing = fields - projected
            if best_missing is None or len(missing) < len(best_missing):
                best_missing = missing
                best_unmapped = missing & unmapped
        assert best_missing is not None
        if not best_missing:
            family["covered"] += 1
            covered_total += 1
            continue
        for field in best_missing:
            if field in best_unmapped:
                family["unmapped"][field] += 1
            else:
                family["unresolved"][field] += 1

    total = len(rules)
    report = {
        "rules": total,
        "claimed": claimed_total,
        "covered": covered_total,
        "families": [],
        "unclaimed": [
            {
                "product": key[0],
                "category": key[1],
                "service": key[2],
                "rules": count,
            }
            for key, count in unclaimed.most_common(args.top)
        ],
    }
    for family in sorted(families.values(), key=lambda f: -f["claimed"]):
        report["families"].append(
            {
                "ids": [document["id"] for document in family["documents"]],
                "selector": family["selector"],
                "claimed": family["claimed"],
                "covered": family["covered"],
                "keyword_only": family["keyword_only"],
                "unresolved": family["unresolved"].most_common(8),
                "unmapped": family["unmapped"].most_common(8),
            }
        )

    if args.json:
        json.dump(report, sys.stdout, indent=2)
        print()
        return 0

    def percent(part: int, whole: int) -> str:
        return f"{100 * part / whole:5.1f}%" if whole else "   n/a"

    print(
        f"rules: {total}  claimed: {claimed_total} ({percent(claimed_total, total)})"
        f"  covered: {covered_total} ({percent(covered_total, total)})"
    )
    print()
    print(f"{'family':<58} {'claimed':>7} {'covered':>7} {'rate':>7}")
    for family in report["families"]:
        ids = ", ".join(family["ids"])
        if len(ids) > 57:
            ids = ids[:54] + "..."
        print(
            f"{ids:<58} {family['claimed']:>7} {family['covered']:>7}"
            f" {percent(family['covered'], family['claimed']):>7}"
        )
        for label in ("unresolved", "unmapped"):
            if family[label]:
                joined = ", ".join(f"{name}:{count}" for name, count in family[label])
                print(f"    {label}: {joined}")
    print()
    print(f"top unclaimed logsources (product, category, service):")
    for entry in report["unclaimed"]:
        print(
            f"  {entry['rules']:>5}  {entry['product']}, {entry['category']},"
            f" {entry['service']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
