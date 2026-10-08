import os
import sys
from pathlib import Path
from typing import NoReturn


def exec_cli(arguments: list[str]) -> NoReturn:
    binary = Path(__file__).with_name("platform-cli")
    os.execv(binary, ["platform-cli", *arguments])


def main() -> NoReturn:
    exec_cli(sys.argv[1:])
