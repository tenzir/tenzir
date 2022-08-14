import pytest

import vast
import vast.backbones

def test_fabric_context_registration(fabric):
    ctx = vast.Context(str, "string")
    fabric.register(ctx)
    ctxs = fabric.context(str)
    assert ctxs == [ctx]

@pytest.mark.asyncio
async def test_fabric_push_pull(fabric):
    fabric.register(vast.Context(int, "int"))
    result = 0
    def assign(x: int):
        nonlocal result
        result = x
    await fabric.pull(assign)
    await fabric.push(42)
    assert result == 42
