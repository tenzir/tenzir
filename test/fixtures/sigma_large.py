"""Generate 1,500-value Sigma selections and boundary probe events."""

import base64
import json
import tempfile
from pathlib import Path

from tenzir_test import fixture


@fixture
def sigma_large():
    with tempfile.TemporaryDirectory(prefix="sigma-large-") as directory:
        root = Path(directory)
        rules = root / "rules"
        rules.mkdir()
        values = [f"needle-{i:04}" for i in range(1500)]
        tokens = [f"t{i:04}" for i in range(1500)]
        selections = {
            "contains": {"CommandLine|contains": values},
            "endswith": {"Image|endswith": values},
            "base64offset": {"CommandLine|base64offset|contains": values},
            "all": {"CommandLine|contains|all": tokens},
            "nested": {"CommandLine|contains": values},
        }
        for title, selection in selections.items():
            detection = {"selection": selection, "condition": "selection"}
            if title == "nested":
                detection.update(
                    block={"CommandLine|contains": ["blocked"]},
                    condition="selection and not block",
                )
            rule = {
                "title": title,
                "logsource": {"category": "process_creation", "product": "windows"},
                "detection": detection,
            }
            # JSON is YAML; no YAML serializer or checked-in generated data needed.
            (rules / f"{title}.yaml").write_text(json.dumps(rule))
        probes = [
            ("first", values[0], "other"),
            ("last", values[-1], values[-1]),
            ("absent", "needle-1500", "needle-1500"),
            ("encoded-last", base64.b64encode(values[-1].encode()).decode(), "other"),
            ("all", " ".join(tokens), "other"),
            ("all-but-last", " ".join(tokens[:-1]), "other"),
            ("blocked", values[-1] + " blocked", "other"),
            ("null", None, None),
        ]
        events = [
            {
                "id": name,
                "class_uid": 1007,
                "metadata": {"version": "1.9.0"},
                "process": {
                    "name": "probe.exe",
                    "cmd_line": command,
                    "file": {"path": image},
                },
            }
            for name, command, image in probes
        ]
        events.append(
            {"id": "missing", "class_uid": 1007, "metadata": {"version": "1.9.0"}}
        )
        (root / "events.json").write_text("\n".join(map(json.dumps, events)))
        yield {"SIGMA_LARGE_ROOT": str(root)}
