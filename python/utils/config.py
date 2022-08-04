import argparse
import dynaconf

def create(config_files):
    return dynaconf.Dynaconf(settings_files=config_files,
        load_dotenv=True,
        envvar_prefix="VAST",
    )

def parse():
    config_files = ["config.yaml", "config.yml"]
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="path to a configuration file")
    args = parser.parse_args()
    if args.config:
        config_files = [args.config]
    return create(config_files)
