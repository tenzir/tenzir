from __future__ import annotations

from tenzir_test.runners import DiffRunner, startup


class InstantiationRunner(DiffRunner):
    def __init__(self) -> None:
        super().__init__(a="--dump-ir", b="--dump-inst-ir", name="instantiation")


@startup()
def _register_instantiation() -> InstantiationRunner:
    return InstantiationRunner()


__all__ = ["InstantiationRunner"]
