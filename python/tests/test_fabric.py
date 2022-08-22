import pytest

@pytest.mark.asyncio
async def test_fabric_convert(fabric):
    # Convert into something unknown.
    result = fabric.convert("foo", 42)
    assert result is None
    # Convert into a registered type.
    fabric.register("int", "string", lambda x: f"answer: {x}")
    result = fabric.convert("int", 42)
    assert result == {"string": "answer: 42"}

@pytest.mark.asyncio
async def test_fabric_push_pull_without_convert(fabric):
    result = 0
    def assign(x: int):
        nonlocal result
        result = x
    await fabric.pull("int", assign)
    await fabric.push("int", 42)
    assert result == 42

@pytest.mark.asyncio
async def test_fabric_push_pull_convert(fabric):
    to_string = lambda x: str(x - 2)
    fabric.register("int", "string", to_string)
    result = "answer: "
    def append_int(x: int):
        nonlocal result
        result += str(x)
    def append_str(x: str):
        nonlocal result
        result += x
    # Register callbacks for the original and converted type. A single push
    # yields two objects.
    await fabric.pull("string", append_str)
    await fabric.pull("int", append_int)
    await fabric.push("int", 4)
    assert result == "answer: 42"
