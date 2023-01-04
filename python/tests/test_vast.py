from vast import VAST

import vast.utils.logging

logger = vast.utils.logging.get(f"vast.{__name__}")


def test_vast():
    v = VAST()
    logger.debug("hello")
    # TODO: make sure we have a vast binary nearby
    assert True
