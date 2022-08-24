from unittest.mock import Mock
import pytest
import stix2

import vast.backbones

@pytest.mark.asyncio
async def test_backbone_pub_sub():
    callback = Mock()
    backbone = vast.backbones.InMemory()
    message = stix2.Bundle()
    topic = "test"
    await backbone.subscribe(topic, callback)
    await backbone.publish(topic, message)
    callback.assert_called_with(message)
