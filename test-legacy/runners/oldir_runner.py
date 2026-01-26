from __future__ import annotations

import typing
from pathlib import Path

from tenzir_test.runners import TqlRunner, startup
from tenzir_test.runners._utils import get_run_module


class OldIrRunner(TqlRunner):
    def __init__(self) -> None:
        super().__init__(name="oldir")

    def run(self, test: Path, update: bool, coverage: bool = False) -> bool | str:
        run_mod = get_run_module()
        test_config = run_mod.parse_test_config(test, coverage=coverage)
        skip_reason = test_config.get("skip")
        if isinstance(skip_reason, str):
            return typing.cast(
                bool | str,
                run_mod.handle_skip(
                    skip_reason,
                    test,
                    update=update,
                    output_ext=self.output_ext,
                ),
            )
        return typing.cast(
            bool | str,
            run_mod.run_simple_test(
                test,
                update=update,
                args=("--dump-pipeline",),
                output_ext=self.output_ext,
                coverage=coverage,
            ),
        )


@startup()
def _register_oldir() -> OldIrRunner:
    return OldIrRunner()


__all__ = ["OldIrRunner"]
