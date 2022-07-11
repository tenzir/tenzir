from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="VAST",
    settings_files=['settings.yaml'],
)
