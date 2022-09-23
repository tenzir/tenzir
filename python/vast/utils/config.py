import argparse
from dynaconf import Dynaconf, Validator

CONFIG_FILES = ["config.yaml", "config.yml"]


def create(config_files=CONFIG_FILES):
    settings = Dynaconf(
        settings_files=config_files,
        load_dotenv=True,
        envvar_prefix="VAST",
    )
    settings.validators.register(
        Validator("fabric.logging.console_verbosity", default="debug"),
        Validator("fabric.logging.file_verbosity", default="quiet"),
        Validator("fabric.logging.filename", default="vast.log"),
    )
    settings.validators.validate_all()
    return settings


def parse():
    config_files = CONFIG_FILES
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="path to a configuration file")
    args = parser.parse_args()
    if args.config:
        config_files = [args.config]
    return create(config_files)
