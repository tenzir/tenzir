from __future__ import annotations

from pathlib import Path

from tenzir_test.runners import TqlRunner, startup
from tenzir_test.runners._utils import get_run_module


class NeoRunner(TqlRunner):
    """TQL runner that uses the neo executor (--neo flag)."""

    def __init__(self) -> None:
        super().__init__(name="neo")

    def run(self, test: Path, update: bool, coverage: bool = False) -> bool | str:
        run_mod = get_run_module()
        return bool(
            run_mod.run_simple_test(
                test,
                update=update,
                args=("--neo",),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        )


@startup()
def _register_neo() -> NeoRunner:
    return NeoRunner()


__all__ = ["NeoRunner"]
