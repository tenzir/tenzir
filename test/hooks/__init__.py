"""Project hooks for tenzir-test."""

from pathlib import Path
import subprocess

from tenzir_test import hooks


def _find_build_script(root: Path) -> Path | None:
    for directory in (root, *root.parents):
        build_script = directory / "scripts" / "build.sh"
        if build_script.is_file():
            return build_script
    return None


@hooks.startup
def use_local_tenzir_build(ctx):
    if ctx.env.get("TENZIR_BINARY") and ctx.env.get("TENZIR_NODE_BINARY"):
        return

    build_script = _find_build_script(ctx.root)
    if build_script is None:
        return

    repo_root = build_script.parent.parent
    result = subprocess.run(
        [str(build_script), "--print-build-dir"],
        cwd=repo_root,
        env=ctx.env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if ctx.debug:
            print(result.stderr.strip())
        return

    build_dir = Path(result.stdout.strip())
    if not build_dir.is_absolute():
        build_dir = repo_root / build_dir

    bin_dir = build_dir / "bin"
    tenzir = bin_dir / "tenzir"
    tenzir_node = bin_dir / "tenzir-node"
    if not tenzir.is_file() or not tenzir_node.is_file():
        if ctx.debug:
            print(f"local Tenzir binaries not found in {bin_dir}")
        return

    ctx.path.insert(0, str(bin_dir))
    ctx.env.setdefault("TENZIR_BINARY", str(tenzir))
    ctx.env.setdefault("TENZIR_NODE_BINARY", str(tenzir_node))

    if ctx.debug:
        print(f"using Tenzir binaries from {bin_dir}")
