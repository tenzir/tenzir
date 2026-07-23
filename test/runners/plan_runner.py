from __future__ import annotations

from pathlib import Path

from tenzir_test.runners import TqlRunner, startup
from tenzir_test.runners._utils import get_run_module


class PlanRunner(TqlRunner):
    """Render the IR plan at a fixed degree of parallelism.

    The parallelism is fixed (rather than `max`) so that the `(x<n>)`
    annotations in the snapshots are deterministic across machines.
    """

    def __init__(self, *, name: str = "plan", parallelism: str = "4") -> None:
        super().__init__(name=name)
        self._parallelism = parallelism

    def run(self, test: Path, update: bool, coverage: bool = False) -> bool | str:
        run_mod = get_run_module()
        return bool(
            run_mod.run_simple_test(
                test,
                update=update,
                args=("--dump-ir-plan", "--parallelism", self._parallelism),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        )


@startup()
def _register_dump_plan_parallel() -> PlanRunner:
    return PlanRunner(name="dump-plan-parallel", parallelism="4")


__all__ = ["PlanRunner"]
