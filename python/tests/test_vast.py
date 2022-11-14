from vast import VAST
import os
import pytest

if "VAST_ENDPOINT" not in os.environ:
    pytest.skip("VAST_ENDPOINT not defined, skipping vast tests", allow_module_level=True)

def test_vast():
    result = VAST.export("").collect()
    assert len(result) == 0
