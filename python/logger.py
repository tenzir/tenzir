import logging
import sys

import coloredlogs
from dynaconf import Dynaconf


def setup(config: Dynaconf, logger: logging.Logger):
    fmt = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
    fmt = '%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s'
    fmt = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
    colored_formatter = coloredlogs.ColoredFormatter(fmt)
    plain_formatter = logging.Formatter(fmt)
    if config.file:
        fh = logging.FileHandler(config.filename)
        fhLevel = logging.getLevelName(config.file_verbosity.upper())
        logger.setLevel(fhLevel)
        fh.setLevel(fhLevel)
        fh.setFormatter(plain_formatter)
        logger.addHandler(fh)
    if config.console:
        ch = logging.StreamHandler()
        chLevel = logging.getLevelName(config.console_verbosity.upper())
        ch.setLevel(chLevel)
        if logger.level > chLevel or logger.level == 0:
            logger.setLevel(chLevel)
        ch.setFormatter(colored_formatter)
        logger.addHandler(ch)
    class ShutdownHandler(logging.Handler):
        """Exit application with CRITICAL logs"""
        def emit(self, _):
            logging.shutdown()
            sys.exit(1)
    sh = ShutdownHandler(level=50)
    sh.setFormatter(colored_formatter)
    logger.addHandler(sh)
