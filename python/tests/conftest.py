import json
import pathlib

import pytest
import stix2

import vast
import vast.backbones
import vast.utils.config

@pytest.fixture
def sighting(request):
    tests = pathlib.Path(request.node.fspath.strpath).parent
    bundle = tests / "data" / "stix-bundle-sighting.json"
    with bundle.open() as f:
        bundle = json.load(f)
        return stix2.parse(bundle)

@pytest.fixture
def fabric():
    config = vast.utils.config.create()
    backbone = vast.backbones.InMemory()
    return vast.Fabric(config, backbone)
