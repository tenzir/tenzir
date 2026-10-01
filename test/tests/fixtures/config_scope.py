# /// script
# dependencies = ["pyyaml"]
# ///

import importlib.util
import os
from pathlib import Path
from tempfile import TemporaryDirectory


root = Path(os.environ["TENZIR_TEST_ROOT"])
spec = importlib.util.spec_from_file_location(
    "config_scope", root / "hooks" / "config_scope.py"
)
assert spec is not None and spec.loader is not None
config_scope = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config_scope)
scope_config = config_scope.scope_tenzir_config

with TemporaryDirectory() as scratch:
    directory = Path(scratch)
    config_file = directory / "tenzir.yaml"
    config_file.write_text("tenzir:\n  nova: true\n")
    scope_file = directory / "tenzir-scope.yaml"
    config_arg = f"--config={config_file}"
    other_arg = "--config=another.yaml"
    args = ["--console-verbosity=warning", config_arg, other_arg]
    env = {"TENZIR_CONFIG": str(config_file), "TENZIR_INPUTS": "inputs"}

    # A directory without a scope retains its existing configuration behavior.
    assert scope_config(directory / "parse_kv.tql", env, args) == (env, args)

    scope_file.write_text("tests:\n  - parse_xml*.tql\n  - parse_winlog.tql\n")
    for name in ("parse_xml.tql", "parse_xml_depth.tql", "parse_winlog.tql"):
        assert scope_config(directory / name, env, args) == (env, args)

    # Excluded files lose only this directory's implicit CLI and environment
    # configuration, not unrelated options or explicitly supplied settings.
    for name in (
        "parse_csv_duplicate_header.tql",
        "parse_csv_trailing_empty.tql",
        "parse_kv.tql",
        "parse_leef.tql",
        "parse_xsv.tql",
        "parse_yaml.tql",
    ):
        actual_env, actual_args = scope_config(directory / name, env, args)
        assert actual_env == {"TENZIR_INPUTS": "inputs"}
        assert actual_args == ["--console-verbosity=warning", other_arg]
    assert env["TENZIR_CONFIG"] == str(config_file)
    assert config_arg in args

    explicit_env = {"TENZIR_CONFIG": "explicit.yaml"}
    assert (
        scope_config(directory / "parse_kv.tql", explicit_env, args)[0] == explicit_env
    )
    assert scope_config(directory / "parse_kv.tql", env, [other_arg]) == (
        env,
        [other_arg],
    )

    # Scopes are directory-local, like tenzir.yaml, and do not affect children.
    child = directory / "nested"
    child.mkdir()
    child_config = child / "tenzir.yaml"
    child_config.write_text("tenzir:\n  nova: true\n")
    child_env = {"TENZIR_CONFIG": str(child_config)}
    child_args = [f"--config={child_config}"]
    assert scope_config(child / "parse_kv.tql", child_env, child_args) == (
        child_env,
        child_args,
    )

    # Invalid scopes must fail rather than silently disable configuration.
    for invalid in ("{}", "[]", "tests: []", "tests: 42", "tests: [42]", 'tests: [""]'):
        scope_file.write_text(invalid)
        try:
            scope_config(directory / "parse_xml.tql", env, args)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted invalid configuration scope: {invalid}")

print("Configuration scope checks passed")
