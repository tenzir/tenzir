from unittest.mock import Mock
import pytest
import stix2

from vast.utils.stix import VAST_IDENTITY

@pytest.mark.asyncio
async def test_fabric_push_pull(fabric):
    bundle = stix2.Bundle(VAST_IDENTITY)
    callback = Mock()
    await fabric.pull(callback)
    await fabric.push(bundle)
    callback.assert_called_once_with(bundle)
    await fabric.push(bundle)
    callback.assert_called_with(bundle)
    assert callback.call_count == 2
