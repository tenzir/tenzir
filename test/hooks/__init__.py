"""Project hooks for tenzir-test."""

import logging
import os
from pathlib import Path
import subprocess
import threading
from typing import Any, Sequence

from tenzir_test import hooks


_LOGGER = logging.getLogger("tenzir_test.cli")


def _should_tee_stderr(args: Sequence[str]) -> bool:
    if not args:
        return False
    executable = os.path.basename(str(args[0]))
    return executable in {"tenzir", "tenzir-node"}


def _install_debug_stderr_tee() -> None:
    import tenzir_test.run as run_mod

    if getattr(run_mod, "_tenzir_debug_stderr_tee_installed", False):
        return
    original_run_subprocess = run_mod.run_subprocess

    def _run_subprocess_with_stderr_tee(
        args: Sequence[str],
        *,
        capture_output: bool,
        check: bool = False,
        text: bool = False,
        force_capture: bool = False,
        stdin_data: bytes | None = None,
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[bytes] | subprocess.CompletedProcess[str]:
        if (
            text
            or not capture_output
            or (run_mod.is_passthrough_enabled() and not force_capture)
            or not _should_tee_stderr(args)
        ):
            return original_run_subprocess(
                args,
                capture_output=capture_output,
                check=check,
                text=text,
                force_capture=force_capture,
                stdin_data=stdin_data,
                **kwargs,
            )
        if any(
            key in kwargs for key in {"stdout", "stderr", "capture_output", "input"}
        ):
            raise TypeError("run_subprocess manages stdout/stderr/input automatically")
        cmd_display = subprocess.list2cmdline([str(arg) for arg in args])
        cwd_value = kwargs.get("cwd")
        cwd_segment = f" (cwd={cwd_value})" if cwd_value else ""
        run_mod._CLI_LOGGER.debug("exec %s%s", cmd_display, cwd_segment)
        timeout = kwargs.pop("timeout", None)
        proc = subprocess.Popen(
            args,
            stdin=subprocess.PIPE if stdin_data is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs,
        )
        stdout_chunks: list[bytes] = []
        stderr_chunks: list[bytes] = []

        def _drain_stdout() -> None:
            assert proc.stdout is not None
            while chunk := proc.stdout.read(65536):
                stdout_chunks.append(chunk)

        def _drain_stderr() -> None:
            assert proc.stderr is not None
            while line := proc.stderr.readline():
                stderr_chunks.append(line)
                text_line = line.decode(errors="replace").rstrip()
                if text_line:
                    run_mod._CLI_LOGGER.debug("%s", text_line)

        stdout_thread = threading.Thread(target=_drain_stdout, daemon=True)
        stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        if stdin_data is not None:
            assert proc.stdin is not None
            try:
                proc.stdin.write(stdin_data)
                proc.stdin.close()
            except BrokenPipeError:
                pass
        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            proc.kill()
            returncode = proc.wait()
            stdout_thread.join()
            stderr_thread.join()
            stdout = b"".join(stdout_chunks)
            stderr = b"".join(stderr_chunks)
            raise subprocess.TimeoutExpired(
                exc.cmd,
                exc.timeout,
                output=stdout,
                stderr=stderr,
            ) from exc
        stdout_thread.join()
        stderr_thread.join()
        stdout = b"".join(stdout_chunks)
        stderr = b"".join(stderr_chunks)
        completed = subprocess.CompletedProcess(args, returncode, stdout, stderr)
        if check and returncode != 0:
            raise subprocess.CalledProcessError(
                returncode,
                args,
                output=stdout,
                stderr=stderr,
            )
        return completed

    run_mod.run_subprocess = _run_subprocess_with_stderr_tee
    run_mod._tenzir_debug_stderr_tee_installed = True


def _find_build_script(root: Path) -> Path | None:
    for directory in (root, *root.parents):
        build_script = directory / "scripts" / "build.sh"
        if build_script.is_file():
            return build_script
    return None


@hooks.startup
def use_local_tenzir_build(ctx):
    if ctx.debug:
        _install_debug_stderr_tee()

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
            _LOGGER.debug(result.stderr.strip())
        return

    build_dir = Path(result.stdout.strip())
    if not build_dir.is_absolute():
        build_dir = repo_root / build_dir

    bin_dir = build_dir / "bin"
    tenzir = bin_dir / "tenzir"
    tenzir_node = bin_dir / "tenzir-node"
    if not tenzir.is_file() or not tenzir_node.is_file():
        if ctx.debug:
            _LOGGER.debug("local Tenzir binaries not found in %s", bin_dir)
        return

    ctx.path.insert(0, str(bin_dir))
    ctx.env.setdefault("TENZIR_BINARY", str(tenzir))
    ctx.env.setdefault("TENZIR_NODE_BINARY", str(tenzir_node))

    if ctx.debug:
        _LOGGER.debug("using Tenzir binaries from %s", bin_dir)
