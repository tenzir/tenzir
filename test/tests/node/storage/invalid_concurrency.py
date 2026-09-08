# runner: python

"""Reject non-positive catalog pool sizes before allocating any workers."""

from __future__ import annotations

import os
import shlex
import subprocess
import tempfile
from pathlib import Path

for setting, diagnostic in (
    ("TENZIR_COMPACTION_SLOTS", "tenzir.compaction-slots"),
    ("TENZIR_PLUGINS__COMPACTION__TIME__STEP_SIZE", "tenzir.compaction-slots"),
    ("TENZIR_CATALOG_LOOKUP_PARALLELISM", "tenzir.catalog-lookup-parallelism"),
):
    for value in ("-1", "0"):
        with tempfile.TemporaryDirectory(prefix="invalid-concurrency-") as tmp:
            root = Path(tmp)
            env = os.environ.copy()
            env.pop("TENZIR_COMPACTION_SLOTS", None)
            env[setting] = value
            result = subprocess.run(
                [
                    *shlex.split(os.environ["TENZIR_NODE_BINARY"]),
                    "--bare-mode",
                    f"--state-directory={root / 'state'}",
                    f"--cache-directory={root / 'cache'}",
                    "--endpoint=false",
                    "--console-verbosity=error",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            assert result.returncode != 0, (setting, value, result.stdout)
            assert f"`{diagnostic}` must be at least 1" in result.stderr, (
                setting,
                value,
                result.stderr,
            )

print("invalid-concurrency: ok")
