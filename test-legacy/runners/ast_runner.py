from __future__ import annotations

import typing
from pathlib import Path

from tenzir_test.runners import TqlRunner, startup
from tenzir_test.runners._utils import get_run_module


class AstRunner(TqlRunner):
    def __init__(self) -> None:
        super().__init__(name="ast")

    def run(self, test: Path, update: bool, coverage: bool = False) -> bool | str:
        run_mod = get_run_module()
        test_config = run_mod.parse_test_config(test, coverage=coverage)
        if test_config.get("skip"):
            return typing.cast(
                bool | str,
                run_mod.handle_skip(
                    str(test_config["skip"]),
                    test,
                    update=update,
                    output_ext=self.output_ext,
                ),
            )
        return bool(
            run_mod.run_simple_test(
                test,
                update=update,
                args=("--dump-ast",),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        )


@startup()
def _register_ast() -> AstRunner:
    return AstRunner()


__all__ = ["AstRunner"]
