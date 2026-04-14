from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping, cast

from tenzir_test.runners import TqlRunner, startup
from tenzir_test.runners._utils import get_run_module


def _split_top_level_chunks(source: str) -> list[str]:
    lines = source.splitlines()
    if any(line.strip() == "---tql" for line in lines):
        return _split_marked_chunks(lines)
    return [source.strip()] if source.strip() else []


def _split_marked_chunks(lines: list[str]) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    for line in lines:
        if line.strip() == "---tql":
            _append_chunk(chunks, current)
            current = []
            continue
        current.append(line)
    _append_chunk(chunks, current)
    return chunks


def _strip_frontmatter(source: str) -> str:
    lines = source.splitlines()
    if not lines or lines[0].strip() != "---":
        return source
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            return "\n".join(lines[index + 1 :])
    return source


def _append_chunk(chunks: list[str], lines: list[str]) -> None:
    chunk = "\n".join(lines).strip()
    if not chunk:
        return
    if not any(
        line.strip() and not line.strip().startswith("//")
        for line in chunk.splitlines()
    ):
        return
    chunks.append(chunk)


class NeoSequentialRunner(TqlRunner):
    """TQL runner that runs neo chunks sequentially without --multi."""

    def __init__(self) -> None:
        super().__init__(name="neo-sequential")

    def run(self, test: Path, update: bool, coverage: bool = False) -> bool | str:
        run_mod = get_run_module()
        fixture_api = run_mod.fixtures_impl
        try:
            test_config = run_mod.parse_test_config(test, coverage=coverage)
        except ValueError as err:
            run_mod.report_failure(test, run_mod.format_failure_message(str(err)))
            return False
        inputs_override = cast(str | None, test_config.get("inputs"))
        env, config_args = run_mod.get_test_env_and_config_args(
            test, inputs=inputs_override
        )
        fixtures = cast(
            tuple[fixture_api.FixtureSpec, ...], test_config.get("fixtures", tuple())
        )
        fixture_assertions_raw = cast(
            Mapping[str, Mapping[str, Any]] | None, test_config.get("assertions")
        )
        fixture_assertions = run_mod._build_fixture_assertions(fixture_assertions_raw)
        timeout = cast(int, test_config["timeout"])
        expect_error = bool(test_config.get("error", False))
        passthrough_mode = run_mod.is_passthrough_enabled()
        package_args = self._package_args(test, test_config, env, run_mod)
        fixture_options = run_mod._build_fixture_options(fixtures)
        context_token = fixture_api.push_context(
            fixture_api.FixtureContext(
                test=test,
                config=cast(dict[str, Any], test_config),
                coverage=coverage,
                env=env,
                config_args=tuple(config_args),
                tenzir_binary=run_mod.TENZIR_BINARY,
                tenzir_node_binary=run_mod.TENZIR_NODE_BINARY,
                fixture_options=fixture_options,
                fixture_assertions=fixture_assertions,
            )
        )
        try:
            with fixture_api.activate(fixtures) as fixture_env:
                env.update(fixture_env)
                run_mod._apply_fixture_env(env, fixtures)
                self._configure_coverage(test, coverage, env)
                node_args = self._node_args(fixtures, env, run_mod)
                chunks = _split_top_level_chunks(_strip_frontmatter(test.read_text()))
                completed = self._run_chunks(
                    chunks,
                    config_args,
                    node_args,
                    package_args,
                    env,
                    timeout,
                    passthrough_mode,
                    run_mod,
                )
                good = completed.returncode == 0
                if expect_error == good:
                    self._report_bad_exit(test, completed, passthrough_mode, run_mod)
                    return False
                if passthrough_mode:
                    return self._finish_passthrough(
                        test, fixtures, fixture_assertions, fixture_api, run_mod
                    )
                if not good:
                    output_ext = "txt"
                else:
                    output_ext = self.output_ext
                output = completed.stdout.replace(
                    (str(run_mod.ROOT) + "/").encode(), b""
                )
                pre_compare = cast(
                    tuple[str, ...], test_config.get("pre_compare", tuple())
                )
                return self._compare_output(
                    test,
                    output,
                    output_ext,
                    update,
                    fixtures,
                    fixture_assertions,
                    fixture_api,
                    run_mod,
                    pre_compare,
                )
        except subprocess.TimeoutExpired:
            run_mod.report_failure(
                test,
                run_mod.format_failure_message(f"subprocess hit {timeout}s timeout"),
            )
            return False
        except subprocess.CalledProcessError as err:
            run_mod.report_failure(
                test, run_mod.format_failure_message(f"subprocess error: {err}")
            )
            return False
        except Exception as err:
            run_mod.report_failure(
                test, run_mod.format_failure_message(f"unexpected exception: {err}")
            )
            return False
        finally:
            fixture_api.pop_context(context_token)
            run_mod.cleanup_test_tmp_dir(env.get(run_mod.TEST_TMP_ENV_VAR))

    def _package_args(
        self,
        test: Path,
        test_config: Mapping[str, Any],
        env: dict[str, str],
        run_mod: Any,
    ) -> list[str]:
        config_package_dirs = cast(
            tuple[str, ...], test_config.get("package_dirs", tuple())
        )
        package_dir_candidates: list[str] = []
        package_root = run_mod.packages.find_package_root(test)
        if package_root is not None:
            env["TENZIR_PACKAGE_ROOT"] = str(package_root)
            package_tests_root = package_root / "tests"
            if test_config.get("inputs") is None:
                nearest = run_mod._find_nearest_inputs_dir(test, package_root)
                env["TENZIR_INPUTS"] = str(
                    nearest.resolve()
                    if nearest is not None
                    else package_tests_root / "inputs"
                )
            package_dir_candidates.append(str(package_root))
        for entry in config_package_dirs:
            package_dir_candidates.extend(run_mod._expand_package_dirs(Path(entry)))
        for cli_path in run_mod._get_cli_packages():
            package_dir_candidates.extend(run_mod._expand_package_dirs(cli_path))
        if not package_dir_candidates:
            return []
        merged_dirs = run_mod._deduplicate_package_dirs(package_dir_candidates)
        env["TENZIR_PACKAGE_DIRS"] = ",".join(merged_dirs)
        return [f"--package-dirs={','.join(merged_dirs)}"]

    def _configure_coverage(
        self, test: Path, coverage: bool, env: dict[str, str]
    ) -> None:
        if not coverage:
            return
        coverage_dir = env.get(
            "CMAKE_COVERAGE_OUTPUT_DIRECTORY", os.path.join(os.getcwd(), "coverage")
        )
        source_dir = env.get("COVERAGE_SOURCE_DIR", os.getcwd())
        os.makedirs(coverage_dir, exist_ok=True)
        env["LLVM_PROFILE_FILE"] = os.path.join(coverage_dir, f"{test.stem}-%p.profraw")
        env["COVERAGE_SOURCE_DIR"] = source_dir

    def _node_args(
        self, fixtures: tuple[Any, ...], env: Mapping[str, str], run_mod: Any
    ) -> list[str]:
        if "node" not in run_mod._fixture_names(fixtures):
            return []
        endpoint = env.get("TENZIR_NODE_CLIENT_ENDPOINT")
        if not endpoint:
            raise RuntimeError(
                "node fixture did not provide TENZIR_NODE_CLIENT_ENDPOINT"
            )
        return [f"--endpoint={endpoint}"]

    def _run_chunks(
        self,
        chunks: list[str],
        config_args: list[str],
        node_args: list[str],
        package_args: list[str],
        env: Mapping[str, str],
        timeout: int,
        passthrough_mode: bool,
        run_mod: Any,
    ) -> subprocess.CompletedProcess[bytes]:
        if not run_mod.TENZIR_BINARY:
            raise RuntimeError("TENZIR_BINARY must be configured before running tests")
        stdout = bytearray()
        stderr = bytearray()
        for chunk in chunks:
            completed = run_mod.run_subprocess(
                [
                    *run_mod.TENZIR_BINARY,
                    "--bare-mode",
                    "--console-verbosity=warning",
                    *config_args,
                    *node_args,
                    *package_args,
                    "--neo",
                    chunk,
                ],
                timeout=timeout,
                env=dict(env),
                capture_output=not passthrough_mode,
                check=False,
                force_capture=not passthrough_mode,
                cwd=str(run_mod.ROOT),
                stdin_data=run_mod.get_stdin_content(env),
            )
            stdout.extend(completed.stdout or b"")
            stderr.extend(completed.stderr or b"")
            if completed.returncode != 0:
                return subprocess.CompletedProcess(
                    completed.args, completed.returncode, bytes(stdout), bytes(stderr)
                )
        return subprocess.CompletedProcess([], 0, bytes(stdout), bytes(stderr))

    def _report_bad_exit(
        self,
        test: Path,
        completed: subprocess.CompletedProcess[bytes],
        passthrough_mode: bool,
        run_mod: Any,
    ) -> None:
        summary = run_mod.format_failure_message(
            f"got unexpected exit code {completed.returncode}"
        )
        if passthrough_mode:
            run_mod.report_failure(test, summary)
            return
        with run_mod.stdout_lock:
            run_mod.fail(test)
            line_prefix = "│ ".encode()
            for line in completed.stdout.splitlines():
                sys.stdout.buffer.write(line_prefix + line + b"\n")
            if completed.stderr:
                sys.stdout.write("├─▶ stderr\n")
                for line in completed.stderr.splitlines():
                    sys.stdout.buffer.write(line_prefix + line + b"\n")
            sys.stdout.write(summary + "\n")

    def _finish_passthrough(
        self,
        test: Path,
        fixtures: tuple[Any, ...],
        fixture_assertions: Any,
        fixture_api: Any,
        run_mod: Any,
    ) -> bool:
        if not fixture_api.is_suite_scope_active(fixtures):
            try:
                run_mod._run_fixture_assertions_for_test(
                    test=test,
                    fixture_specs=fixtures,
                    fixture_assertions=fixture_assertions,
                )
            except Exception as err:
                run_mod.report_failure(
                    test, run_mod._fixture_assertion_failure_message(err)
                )
                return False
        run_mod.success(test)
        return True

    def _compare_output(
        self,
        test: Path,
        output: bytes,
        output_ext: str,
        update: bool,
        fixtures: tuple[Any, ...],
        fixture_assertions: Any,
        fixture_api: Any,
        run_mod: Any,
        pre_compare: tuple[str, ...],
    ) -> bool:
        ref_path = test.with_suffix(f".{output_ext}")
        if update:
            ref_path.write_bytes(output)
        else:
            if not ref_path.exists():
                run_mod.report_failure(
                    test,
                    run_mod.format_failure_message(
                        f'Failed to find ref file: "{ref_path}"'
                    ),
                )
                return False
            run_mod.log_comparison(test, ref_path, mode="comparing")
            expected = ref_path.read_bytes()
            expected_transformed = run_mod.apply_pre_compare(expected, pre_compare)
            output_transformed = run_mod.apply_pre_compare(output, pre_compare)
            if expected_transformed != output_transformed:
                if run_mod.interrupt_requested():
                    run_mod.report_interrupted_test(test)
                else:
                    run_mod.report_failure(test, "")
                    run_mod.print_diff(
                        expected_transformed, output_transformed, ref_path
                    )
                return False
        return self._finish_passthrough(
            test, fixtures, fixture_assertions, fixture_api, run_mod
        )


@startup()
def _register_neo_sequential() -> NeoSequentialRunner:
    return NeoSequentialRunner()


__all__ = ["NeoSequentialRunner"]
