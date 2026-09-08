# runner: python

"""A configured package whose pipeline fails inside a package operator.

The failing location lives in the operator's body, not in the pipeline
definition. The node must therefore report the operator's file, and point at
the invocation only as the call site.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import textwrap
import time
from pathlib import Path


def _node_binary() -> list[str]:
    if env_value := os.environ.get("TENZIR_NODE_BINARY"):
        return shlex.split(env_value)
    if binary := shutil.which("tenzir-node"):
        return [binary]
    raise RuntimeError("tenzir-node executable not found")


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    package_dir = root / "packages" / "brokenpkg"
    operators_dir = package_dir / "operators"
    operators_dir.mkdir(parents=True)
    # The operator body only fails once the pipeline below invokes it.
    operator_file = operators_dir / "boom.tql"
    operator_file.write_text(
        textwrap.dedent(
            """\
            nonexistent_in_body
            """
        ),
        encoding="utf-8",
    )
    (package_dir / "package.yaml").write_text(
        textwrap.dedent(
            """\
            id: brokenpkg
            name: Broken Package
            pipelines:
              boomer:
                definition: |
                  from {x: 1}
                  brokenpkg::boom
                  discard
            """
        ),
        encoding="utf-8",
    )
    config = root / "tenzir.yaml"
    config.write_text(
        textwrap.dedent(
            f"""\
            tenzir:
              package-dirs:
                - {root / "packages"}
            """
        ),
        encoding="utf-8",
    )
    proc = subprocess.Popen(
        [
            *_node_binary(),
            f"--config={config}",
            f"--state-directory={root / 'state'}",
            f"--cache-directory={root / 'cache'}",
            "--endpoint=false",
            "--console-verbosity=error",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 60
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        stdout, stderr = proc.communicate()
        raise AssertionError(
            "node kept running despite a broken configured package\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )
    stdout, stderr = proc.communicate()
    assert proc.returncode != 0, (proc.returncode, stdout, stderr)
    # The primary location must resolve in the operator's own file, and the
    # call site must name the pipeline that invoked it.
    assert f"--> {operator_file}:1:" in stderr, stderr
    assert "--> <packages/brokenpkg/boomer>:2:" in stderr, stderr
    assert "called from here" in stderr, stderr

print("package-operator-diagnostic: ok")
