from dynaconf import Dynaconf, Validator

# TODO: make the path configurable
CONFIG_FILES = ["config.yaml", "config.yml"]
PREFIX = "python."


class Config:
    def __init__(self, conf: Dynaconf):
        self.conf = conf

    def get(self, key):
        return self.conf.get(PREFIX + key)


def create() -> Config:
    settings = Dynaconf(
        settings_files=CONFIG_FILES,
        load_dotenv=True,
        envvar_prefix="VAST",
    )
    settings.validators.register(
        Validator(PREFIX + "console-verbosity", default="debug"),
        Validator(PREFIX + "file-verbosity", default="quiet"),
        Validator(PREFIX + "log-file", default="vast.log"),
    )
    settings.validators.validate_all()
    return Config(settings)
