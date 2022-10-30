from unittest.mock import Mock
import pytest
import stix2

import vast
from vast.utils.stix import IDENTITIES


@pytest.fixture
def fabric():
    config = vast.utils.config.create()
    backbone = vast.fabric.backbones.InMemory()
    return vast.fabric.Fabric(config, backbone)


@pytest.mark.asyncio
async def test_fabric_push_pull(fabric):
    bundle = stix2.Bundle(IDENTITIES["vast"])
    callback = Mock()
    await fabric.pull(callback)
    await fabric.push(bundle)
    callback.assert_called_once_with(bundle)
    await fabric.push(bundle)
    callback.assert_called_with(bundle)
    assert callback.call_count == 2
