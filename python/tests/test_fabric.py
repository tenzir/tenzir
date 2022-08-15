import pytest

import vast
import vast.backbones

def test_fabric_context_registration(fabric):
    fabric.register(str, "string")
    assert fabric.registry[str][0].type == str
    assert fabric.registry[str][0].name == "string"

@pytest.mark.asyncio
async def test_fabric_push_pull(fabric):
    fabric.register(int, "int")
    result = 0
    def assign(x: int):
        nonlocal result
        result = x
    await fabric.pull(assign)
    await fabric.push(42)
    assert result == 42
