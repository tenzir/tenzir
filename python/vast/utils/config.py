from dynaconf import Dynaconf, Validator

# TODO: make the path configurable
CONFIG_FILES = ["config.yaml", "config.yml"]


def create():
    settings = Dynaconf(
        settings_files=CONFIG_FILES,
        load_dotenv=True,
        envvar_prefix="VAST",
    )
    settings.validators.register(
        Validator("console_verbosity", default="debug"),
        Validator("file_verbosity", default="quiet"),
        Validator("filename", default="vast.log"),
    )
    settings.validators.validate_all()
    return settings
