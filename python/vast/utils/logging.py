import logging
import sys

import coloredlogs
from dynaconf import Dynaconf

import vast.utils.config


def configure(config: Dynaconf, logger: logging.Logger):
    fmt = "%(asctime)s %(name)s %(levelname)-7s %(message)s"
    colored_formatter = coloredlogs.ColoredFormatter(fmt)
    plain_formatter = logging.Formatter(fmt)
    if config.file_verbosity != "quiet":
        fh = logging.FileHandler(config.filename)
        fh_level = logging.getLevelName(config.file_verbosity.upper())
        logger.setLevel(fh_level)
        fh.setLevel(fh_level)
        fh.setFormatter(plain_formatter)
        logger.addHandler(fh)
    if config.console_verbosity != "quiet":
        ch = logging.StreamHandler()
        ch_level = logging.getLevelName(config.console_verbosity.upper())
        ch.setLevel(ch_level)
        if logger.level > ch_level or logger.level == 0:
            logger.setLevel(ch_level)
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


def get(name=None):
    # All VAST loggers (vast.*) have handlers. If not, the logger is not yet
    # initialized and we do it lazily at the first use here.
    vast_logger = logging.getLogger("vast")
    if not vast_logger.hasHandlers():
        config = vast.utils.config.parse()
        configure(config.fabric.logging, vast_logger)
    return logging.getLogger(name)
