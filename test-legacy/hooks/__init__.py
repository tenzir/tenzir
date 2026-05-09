"""Project hooks for legacy integration tests."""

import os
from typing import Any, Sequence

from tenzir_test import hooks


def _is_tenzir_command(args: Sequence[str]) -> bool:
    if not args:
        return False
    executable = os.path.basename(str(args[0]))
    return executable in {"tenzir", "tenzir-node", "tenzir-ctl"}


@hooks.startup
def disable_neo_executor(ctx):
    # FIXME: This is a temporary monkeypatch to keep the lingering legacy tests
    # running without poisoning tenzir-test's own neo-based version probe. Remove
    # it soon, once the remaining tests have moved to `test/` or have been
    # deleted.
    import tenzir_test.run as run_mod

    if getattr(run_mod, "_tenzir_legacy_executor_hook_installed", False):
        return

    original_run_subprocess = run_mod.run_subprocess

    def _run_legacy_tenzir_subprocess(
        args: Sequence[str],
        **kwargs: Any,
    ):
        if _is_tenzir_command(args):
            env = dict(kwargs.get("env") or ctx.env)
            env.setdefault("TENZIR_NEO", "false")
            kwargs["env"] = env
        return original_run_subprocess(args, **kwargs)

    run_mod.run_subprocess = _run_legacy_tenzir_subprocess
    run_mod._tenzir_legacy_executor_hook_installed = True
