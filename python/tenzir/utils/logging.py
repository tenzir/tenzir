import logging
import sys

from tenzir.utils.config import create as create_config, Config


def configure(config: Config, logger: logging.Logger):
    fmt = "%(asctime)s %(name)s %(levelname)-7s %(message)s"
    plain_formatter = logging.Formatter(fmt)
    if config.get("file-verbosity") != "quiet":
        fh = logging.FileHandler(config.get("log-file"))
        fh_level = logging.getLevelName(config.get("file-verbosity").upper())
        logger.setLevel(fh_level)
        fh.setLevel(fh_level)
        fh.setFormatter(plain_formatter)
        logger.addHandler(fh)
    if config.get("console-verbosity") != "quiet":
        ch = logging.StreamHandler()
        ch_level = logging.getLevelName(config.get("console-verbosity").upper())
        ch.setLevel(ch_level)
        if logger.level > ch_level or logger.level == 0:
            logger.setLevel(ch_level)
        ch.setFormatter(plain_formatter)
        logger.addHandler(ch)

    class ShutdownHandler(logging.Handler):
        """Exit application with CRITICAL logs"""

        def emit(self, _):
            logging.shutdown()
            sys.exit(1)

    sh = ShutdownHandler(level=50)
    sh.setFormatter(plain_formatter)
    logger.addHandler(sh)


def get(name=None):
    """Get a logger instance while ensuring that the Tenzir logger namespace
    (loggers with name "tenzir or "tenzir.*") is properly configured"""
    # The logger "tenzir" is the root of the namespace. All loggers names tenzir.*
    # will inherit its settings
    tenzir_logger = logging.getLogger("tenzir")
    # Setting "propagate" to false disables inheritance from the root logger
    tenzir_logger.propagate = False
    # If the logger has no handlers, it means it hasn't been configured yet.
    if not tenzir_logger.hasHandlers():
        configure(create_config(), tenzir_logger)
    return logging.getLogger(name)
