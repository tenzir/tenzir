from __future__ import annotations

from tenzir_test.runners import DiffRunner, startup


class OptRunner(DiffRunner):
    def __init__(self) -> None:
        super().__init__(a="--dump-inst-ir", b="--dump-opt-ir", name="opt")


@startup()
def _register_opt() -> OptRunner:
    return OptRunner()


__all__ = ["OptRunner"]
