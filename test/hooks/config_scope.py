"""Opt-in filename scopes for directory-local Tenzir configuration."""

from fnmatch import fnmatchcase
from pathlib import Path

import yaml


def scope_tenzir_config(
    test: Path, env: dict[str, str], config_args: list[str]
) -> tuple[dict[str, str], list[str]]:
    """An adjacent tenzir-scope.yaml lists selected filename globs in tests."""
    config_file = test.parent / "tenzir.yaml"
    config_arg = f"--config={config_file}"
    scope_file = test.parent / "tenzir-scope.yaml"
    if config_arg not in config_args or not scope_file.is_file():
        return env, config_args

    scope = yaml.safe_load(scope_file.read_text())
    patterns = scope.get("tests") if isinstance(scope, dict) else None
    if (
        not isinstance(patterns, list)
        or not patterns
        or any(not isinstance(pattern, str) or not pattern for pattern in patterns)
    ):
        raise ValueError(
            f"{scope_file}: expected a non-empty tests list of filename globs"
        )
    if any(fnmatchcase(test.name, pattern) for pattern in patterns):
        return env, config_args

    env = env.copy()
    if env.get("TENZIR_CONFIG") == str(config_file):
        del env["TENZIR_CONFIG"]
    return env, [arg for arg in config_args if arg != config_arg]
