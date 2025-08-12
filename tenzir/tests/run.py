#!/usr/bin/env python3

import argparse
import builtins
import dataclasses
import difflib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import typing
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, override

TENZIR_BINARY = shutil.which("tenzir")
TENZIR_NODE_BINARY = shutil.which("tenzir-node")
ROOT = Path(os.path.dirname(__file__ or ".")).resolve()
INPUTS_DIR = ROOT / "inputs"
CHECKMARK = "\033[92;1m✓\033[0m"
CROSS = "\033[31m✘\033[0m"
INFO = "\033[94;1mi\033[0m"

stdout_lock = threading.RLock()


def get_test_env_and_config_args(test):
    config_file = test.parent / "tenzir.yaml"
    config_args = [f"--config={config_file}"] if config_file.exists() else []
    env = os.environ.copy()
    env["INPUTS"] = str(INPUTS_DIR)
    return env, config_args


def report_failure(test, message):
    with stdout_lock:
        fail(test)
        if message:
            print(message)


def parse_test_config(test_file: Path, coverage=False):
    """Parse test configuration from TQL comments at the beginning of the file."""
    # Define valid configuration keys and their default values
    config = {
        "error": False,
        "timeout": 30,  # Default timeout of 30 seconds
        "test": "exec",  # Default to exec runner
        "node": False,  # Default to not using a node
        "skip": None,  # Optional skip reason
    }

    valid_keys = set(config.keys())

    is_tql = test_file.suffix == ".tql"
    is_py = test_file.suffix == ".py"

    with open(test_file, "r", errors="ignore") as f:
        line_number = 0
        for line in f:
            line_number += 1
            line = line.strip()
            # End of frontmatter
            if not line.startswith("//") and is_tql:
                break
            if not line.startswith("#") and is_py:
                break

            # Remove the comment prefix and strip whitespace
            content = line[2:].strip()

            # Check if this looks like a configuration line (key: value)
            parts = content.split(":", 1)
            if len(parts) != 2:
                # Gracefully handle files without frontmatter that start with a comment.
                if line_number == 1:
                    break
                raise ValueError(
                    f"Error in {test_file}:{line_number}: Invalid frontmatter, expected 'key: value'"
                )

            key = parts[0].strip()
            value = parts[1].strip()

            # Check for unknown keys
            if key not in valid_keys:
                raise ValueError(
                    f"Error in {test_file}:{line_number}: Unknown configuration key '{key}'"
                )

            # Convert value to appropriate type
            if key == "skip":
                if not value:
                    raise ValueError(
                        f"Error in {test_file}:{line_number}: 'skip' value must be a non-empty string"
                    )
                config[key] = value
                continue
            if key == "error":
                if value.lower() == "true":
                    config[key] = True
                    continue
                if value.lower() == "false":
                    config[key] = False
                    continue
                raise ValueError(
                    f"Error in {test_file}:{line_number}: Invalid value for 'error', expected 'true' or 'false', got '{value}'"
                )
            if key == "timeout":
                try:
                    timeout_value = int(value)
                    if timeout_value <= 0:
                        raise ValueError(
                            f"Error in {test_file}:{line_number}: Invalid value for 'timeout', expected positive integer, got '{value}'"
                        )
                    config[key] = timeout_value
                    continue
                except ValueError as e:
                    if "Error in" in str(e):
                        raise
                    raise ValueError(
                        f"Error in {test_file}:{line_number}: Invalid value for 'timeout', expected integer, got '{value}'"
                    )
            if key == "test":
                # Valid runner types
                valid_runners = [runner.prefix for runner in RUNNERS]
                if value not in valid_runners:
                    raise ValueError(
                        f"Error in {test_file}:{line_number}: Invalid value for 'test', expected one of {valid_runners}, got '{value}'"
                    )
                config[key] = value
                continue
            if key == "node":
                if value.lower() == "true":
                    config[key] = True
                    continue
                if value.lower() == "false":
                    config[key] = False
                    continue
                raise ValueError(
                    f"Error in {test_file}:{line_number}: Invalid value for 'node', expected 'true' or 'false', got '{value}'"
                )

    # If coverage is enabled, increase timeout by a factor of 5
    if coverage:
        config["timeout"] *= 5

    return config


def print(*args, **kwargs):
    # TODO: Properly solve the synchronization below.
    if "flush" not in kwargs:
        kwargs["flush"] = True
    return builtins.print(*args, **kwargs)


@dataclasses.dataclass
class Summary:
    def __init__(self, failed: int = 0, total: int = 0, skipped: int = 0):
        self.failed = failed
        self.total = total
        self.skipped = skipped

    def __add__(self, other: "Summary") -> "Summary":
        return Summary(
            self.failed + other.failed,
            self.total + other.total,
            self.skipped + other.skipped,
        )


def get_version() -> str:
    return (
        subprocess.check_output(
            [
                TENZIR_BINARY,
                "--bare-mode",
                "--console-verbosity=warning",
                "version | select version | write_lines",
            ]
        )
        .decode()
        .strip()
    )


def success(test: Path) -> None:
    with stdout_lock:
        print(f"{CHECKMARK} {test.relative_to(ROOT)}")


def fail(test: Path) -> None:
    with stdout_lock:
        print(f"{CROSS} {test.relative_to(ROOT)}")


def last_and(gen):
    previous = None
    gen = iter(gen)
    for x in gen:
        previous = x
        break
    for x in gen:
        yield (False, previous)
        previous = x
    yield (True, previous)


def print_diff(expected: bytes, actual: bytes, path: Path):
    diff = list(
        difflib.diff_bytes(
            difflib.unified_diff,
            expected.splitlines(keepends=True),
            actual.splitlines(keepends=True),
            n=2,
        )
    )
    with stdout_lock:
        skip = 2
        for i, line in enumerate(diff):
            if skip > 0:
                skip -= 1
                continue
            if line.startswith(b"@@"):
                print(f"┌─▶ \033[31m{path.relative_to(ROOT)}\033[0m")
                continue
            if line.startswith(b"+"):
                line = b"\033[92m" + line + b"\033[0m"
            elif line.startswith(b"-"):
                line = b"\033[31m" + line + b"\033[0m"
            prefix = ("│ " if i != len(diff) - 1 else "└─").encode()
            sys.stdout.buffer.write(prefix + line)


def check_group_is_empty(pgid: int):
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return
    raise ValueError("leftover child processes!")


def run_simple_test(
    test: Path,
    *,
    update: bool,
    args: typing.Sequence[str] = (),
    output_ext: str,
    coverage: bool = False,
) -> bool:
    try:
        # Parse test configuration
        test_config = parse_test_config(test, coverage=coverage)
    except ValueError as e:
        report_failure(test, f"└─▶ \033[31m{e}\033[0m")
        return False

    try:
        env, config_args = get_test_env_and_config_args(test)

        # Set up environment for code coverage if enabled
        if coverage:
            coverage_dir = os.environ.get(
                "CMAKE_COVERAGE_OUTPUT_DIRECTORY", os.path.join(os.getcwd(), "coverage")
            )
            source_dir = os.environ.get("COVERAGE_SOURCE_DIR", os.getcwd())
            os.makedirs(coverage_dir, exist_ok=True)
            test_name = test.stem
            profile_path = os.path.join(coverage_dir, f"{test_name}-%p.profraw")
            env["LLVM_PROFILE_FILE"] = profile_path
            env["COVERAGE_SOURCE_DIR"] = source_dir

        cmd = [
            TENZIR_BINARY,
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
            *[x for x in config_args if x is not None],
            *[x for x in args if x is not None],
            "-f",
            str(test),
        ]
        completed = subprocess.run(
            cmd,
            timeout=test_config["timeout"],
            stdout=subprocess.PIPE,
            env=env,
        )
        output = completed.stdout
        output = output.replace(str(ROOT).encode() + b"/", b"")
        good = completed.returncode == 0
    except subprocess.TimeoutExpired:
        report_failure(
            test, f'└─▶ \033[31msubprocess hit {test_config["timeout"]}s timeout\033[0m'
        )
        return False
    except subprocess.CalledProcessError as e:
        report_failure(test, f"└─▶ \033[31msubprocess error: {e}\033[0m")
        return False
    except Exception as e:
        report_failure(test, f"└─▶ \033[31munexpected exception: {e}\033[0m")
        return False
    if test_config["error"] == good:
        with stdout_lock:
            report_failure(
                test,
                f"┌─▶ \033[31mgot unexpected exit code {completed.returncode}\033[0m",
            )
            for last, line in last_and(output.split(b"\n")):
                prefix = "│ " if not last else "└─"
                sys.stdout.buffer.write(prefix.encode() + line + b"\n")
        return False
    if not good:
        output_ext = "txt"
    ref_path = test.with_suffix(f".{output_ext}")
    if update:
        with ref_path.open("wb") as f:
            f.write(output)
    else:
        if not ref_path.exists():
            report_failure(
                test, f'└─▶ \033[31mFailed to find ref file: "{ref_path}"\033[0m'
            )
            return False
        expected = ref_path.read_bytes()
        if expected != output:
            report_failure(test, "")
            print_diff(expected, output, ref_path)
            return False
    success(test)
    return True


class Runner(ABC):
    def __init__(self, *, prefix: str):
        self._prefix = prefix

    @property
    def prefix(self) -> str:
        return self._prefix

    def collect_with_ext(self, path: Path, ext: str) -> set:
        todo = set()
        if path.relative_to(ROOT).parts[0] != self._prefix:
            raise ValueError(
                f"test path `{path}` should` not be collected via fixture `{self._prefix}`"
            )
        if path.suffix == f".{ext}":
            todo.add((self, path))
            return todo
        for test in path.glob(f"**/*.{ext}"):
            todo.add((self, test))
        return todo

    @abstractmethod
    def collect_tests(self, path: Path):
        raise NotImplementedError

    @abstractmethod
    def purge(self):
        raise NotImplementedError

    @abstractmethod
    def run(self, test: Path, update: bool) -> typing.Union[bool, str]:
        raise NotImplementedError


class ExtRunner(Runner):
    def __init__(self, *, prefix: str, ext: str):
        super().__init__(prefix=prefix)
        self._ext = ext

    def collect_tests(self, path: Path):
        return self.collect_with_ext(path, self._ext)

    def purge(self):
        purge_base = ROOT / self._prefix
        for p in purge_base.rglob("*"):
            if p.is_dir():
                continue
            if p.suffix == f".{self._ext}":
                continue
            # print(f"would have deleted {p}")
            p.unlink()


class TqlRunner(ExtRunner):
    output_ext: str = "txt"

    def __init__(self, *, prefix: str):
        super().__init__(prefix=prefix, ext="tql")


def handle_skip(
    reason: str, test: Path, update: bool, output_ext: str
) -> typing.Union[bool, str]:
    rel_path = test.relative_to(ROOT)
    print(f"{INFO} skipped {rel_path}: {reason}")
    ref_path = test.with_suffix(f".{output_ext}")
    if update:
        # Always overwrite reference file with empty content
        with ref_path.open("wb") as f:
            f.write(b"")
    else:
        # If reference file exists, it must be empty
        if ref_path.exists():
            expected = ref_path.read_bytes()
            if expected != b"":
                report_failure(
                    test,
                    f'└─▶ \033[31mReference file for skipped test must be empty: "{ref_path}"\033[0m',
                )
                return False
    return "skipped"


class LexerRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="lexer")

    @override
    def run(
        self, test: Path, update: bool, coverage: bool = False
    ) -> typing.Union[bool, str]:
        test_config = parse_test_config(test, coverage=coverage)
        if test_config.get("skip"):
            return handle_skip(
                str(test_config["skip"]),
                test,
                update=update,
                output_ext=self.output_ext,
            )
        if test_config.get("node", False):
            return run_with_node(
                test,
                update=update,
                args=("--dump-tokens",),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        return run_simple_test(
            test,
            update=update,
            args=("--dump-tokens",),
            output_ext=self.output_ext,
            coverage=coverage,
        )


class AstRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="ast")

    @override
    def run(self, test: Path, update: bool, coverage: bool = False) -> bool | str:
        test_config = parse_test_config(test, coverage=coverage)
        if test_config.get("skip"):
            return handle_skip(
                str(test_config["skip"]),
                test,
                update=update,
                output_ext=self.output_ext,
            )
        if test_config.get("node", False):
            return run_with_node(
                test,
                update=update,
                args=("--dump-ast",),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        return run_simple_test(
            test,
            update=update,
            args=("--dump-ast",),
            output_ext=self.output_ext,
            coverage=coverage,
        )


class OldIrRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="oldir")

    @override
    def run(
        self, test: Path, update: bool, coverage: bool = False
    ) -> typing.Union[bool, str]:
        test_config = parse_test_config(test, coverage=coverage)
        if test_config.get("skip"):
            return handle_skip(
                str(test_config["skip"]),
                test,
                update=update,
                output_ext=self.output_ext,
            )
        if test_config.get("node", False):
            return run_with_node(
                test,
                update=update,
                args=("--dump-pipeline",),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        return run_simple_test(
            test,
            update=update,
            args=("--dump-pipeline",),
            output_ext=self.output_ext,
            coverage=coverage,
        )


class IrRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="ir")

    @override
    def run(
        self, test: Path, update: bool, coverage: bool = False
    ) -> typing.Union[bool, str]:
        test_config = parse_test_config(test, coverage=coverage)
        if test_config.get("skip"):
            return handle_skip(
                str(test_config["skip"]),
                test,
                update=update,
                output_ext=self.output_ext,
            )
        if test_config.get("node", False):
            return run_with_node(
                test,
                update=update,
                args=("--dump-ir",),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        return run_simple_test(
            test,
            update=update,
            args=("--dump-ir",),
            output_ext=self.output_ext,
            coverage=coverage,
        )


class DiffRunner(TqlRunner):
    def __init__(self, *, a: str, b: str, prefix: str):
        super().__init__(prefix=prefix)
        self._a = a
        self._b = b

    @override
    def run(
        self, test: Path, update: bool, coverage: bool = False
    ) -> typing.Union[bool, str]:
        test_config = parse_test_config(test, coverage=coverage)
        if test_config.get("skip"):
            return handle_skip(
                str(test_config["skip"]),
                test,
                update=update,
                output_ext=self.output_ext,
            )
        if test_config.get("node", False):
            return run_with_node_diff(
                test, update=update, a=self._a, b=self._b, coverage=coverage
            )
        try:
            pass
        except ValueError as e:
            report_failure(test, f"└─▶ \033[31m{e}\033[0m")
        env, _ = get_test_env_and_config_args(test)

        unoptimized = subprocess.run(
            [TENZIR_BINARY, self._a, "-f", str(test)],
            timeout=test_config["timeout"],
            stdout=subprocess.PIPE,
            env=env,
        )
        optimized = subprocess.run(
            [TENZIR_BINARY, self._b, "-f", str(test)],
            timeout=test_config["timeout"],
            stdout=subprocess.PIPE,
            env=env,
        )
        diff = list(
            difflib.diff_bytes(
                difflib.unified_diff,
                unoptimized.stdout.splitlines(keepends=True),
                optimized.stdout.splitlines(keepends=True),
                n=2**31 - 1,
            )
        )[3:]
        if diff:
            diff = b"".join(diff)
        else:
            diff = b"".join(
                map(lambda x: b" " + x, unoptimized.stdout.splitlines(keepends=True))
            )
        ref_path = test.with_suffix(".diff")
        if update:
            ref_path.write_bytes(diff)
        else:
            expected = ref_path.read_bytes()
            if diff != expected:
                report_failure(test, "")
                print_diff(expected, diff, ref_path)
                return False
        success(test)
        return True


class InstantiationRunner(DiffRunner):
    def __init__(self):
        super().__init__(a="--dump-ir", b="--dump-inst-ir", prefix="instantiation")


class OptRunner(DiffRunner):
    def __init__(self):
        super().__init__(a="--dump-inst-ir", b="--dump-opt-ir", prefix="opt")


class FinalizeRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="finalize")

    @override
    def run(
        self, test: Path, update: bool, coverage: bool = False
    ) -> typing.Union[bool, str]:
        test_config = parse_test_config(test, coverage=coverage)
        if test_config.get("skip"):
            return handle_skip(
                str(test_config["skip"]),
                test,
                update=update,
                output_ext=self.output_ext,
            )
        if test_config.get("node", False):
            return run_with_node(
                test,
                update=update,
                args=("--dump-finalized",),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        return run_simple_test(
            test,
            update=update,
            args=("--dump-finalized",),
            output_ext=self.output_ext,
            coverage=coverage,
        )


class ExecRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="exec")

    @override
    def run(
        self, test: Path, update: bool, coverage: bool = False
    ) -> typing.Union[bool, str]:
        test_config = parse_test_config(test, coverage=coverage)
        if test_config.get("skip"):
            return handle_skip(
                str(test_config["skip"]),
                test,
                update=update,
                output_ext=self.output_ext,
            )
        if test_config.get("node", False):
            return run_with_node(
                test,
                update=update,
                args=(),
                output_ext=self.output_ext,
                coverage=coverage,
            )
        return run_simple_test(
            test, update=update, output_ext=self.output_ext, coverage=coverage
        )


@contextmanager
def tenzir_node_endpoint(test: Path, coverage: bool = False):
    # Parse test configuration with coverage flag to adjust timeout
    test_config = parse_test_config(test, coverage=coverage)

    # Check if tenzir-node binary is available
    if not TENZIR_NODE_BINARY:
        report_failure(test, f"└─▶ \033[31mCould not find tenzir-node binary\033[0m")
        yield None
        return

    node_process = None
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            env, config_args = get_test_env_and_config_args(test)

            # Set up environment for code coverage if enabled
            if coverage:
                coverage_dir = os.environ.get(
                    "CMAKE_COVERAGE_OUTPUT_DIRECTORY",
                    os.path.join(os.getcwd(), "coverage"),
                )
                source_dir = os.environ.get("COVERAGE_SOURCE_DIR", os.getcwd())
                os.makedirs(coverage_dir, exist_ok=True)
                test_name = test.stem + "-node"
                profile_path = os.path.join(coverage_dir, f"{test_name}-%p.profraw")
                env["LLVM_PROFILE_FILE"] = profile_path
                env["COVERAGE_SOURCE_DIR"] = source_dir

            node_cmd = [
                TENZIR_NODE_BINARY,
                "--bare-mode",
                "--console-verbosity=warning",
                f"--state-directory={Path(temp_dir) / 'state'}",
                f"--cache-directory={Path(temp_dir) / 'cache'}",
                "--endpoint=localhost:0",
                "--print-endpoint",
                *[x for x in config_args if x is not None],
            ]
            node_process = subprocess.Popen(
                node_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=env,
                start_new_session=True,
            )

            # Wait for the node to print its endpoint and parse it
            endpoint = None
            for line in node_process.stdout:
                endpoint = line.strip()
                break

            if not endpoint:
                report_failure(
                    test, f"└─▶ \033[31mFailed to get endpoint from tenzir-node"
                )
                yield None
                return

            yield endpoint

        except Exception as e:
            report_failure(test, f"└─▶ \033[31mFailed to start node: {e}\033[0m")
            yield None
        finally:
            if node_process:
                try:
                    pgid = os.getpgid(node_process.pid)
                    node_process.terminate()
                    _ = node_process.wait(timeout=20)
                    check_group_is_empty(pgid)
                except subprocess.TimeoutExpired as e:
                    report_failure(
                        test,
                        f"└─▶ \033[31mError terminating node process within 20s: {e}",
                    )
                    node_process.kill()
                    _ = node_process.wait()
                except Exception as e:
                    report_failure(
                        test,
                        f"└─▶ \033[31mError terminating node process: {type(e)}:{e}",
                    )


def run_with_node(
    test: Path,
    update: bool,
    args: typing.Sequence[str],
    output_ext: str,
    coverage: bool = False,
) -> bool:
    # Parse test configuration with coverage flag to adjust timeout
    test_config = parse_test_config(test, coverage=coverage)

    with tenzir_node_endpoint(test, coverage=coverage) as endpoint:
        if not endpoint:
            return False
        try:
            cmd_args = [f"--endpoint={endpoint}", *[x for x in args if x is not None]]
            result = run_simple_test(
                test,
                update=update,
                args=cmd_args,
                output_ext=output_ext,
                coverage=coverage,
            )
            return result
        except Exception as e:
            report_failure(test, f"└─▶ \033[31mFailed to run node test: {e}\033[0m")
            return False


def run_with_node_diff(
    test: Path, *, update: bool, a: str, b: str, coverage: bool = False
) -> bool:
    # Parse test configuration with coverage flag to adjust timeout
    test_config = parse_test_config(test, coverage=coverage)
    env, _ = get_test_env_and_config_args(test)

    # Set up environment for code coverage if enabled
    if coverage:
        coverage_dir = os.environ.get(
            "CMAKE_COVERAGE_OUTPUT_DIRECTORY", os.path.join(os.getcwd(), "coverage")
        )
        source_dir = os.environ.get("COVERAGE_SOURCE_DIR", os.getcwd())
        os.makedirs(coverage_dir, exist_ok=True)
        test_name = test.stem
        profile_path = os.path.join(coverage_dir, f"{test_name}-unopt-%p.profraw")
        env["LLVM_PROFILE_FILE"] = profile_path
        env["COVERAGE_SOURCE_DIR"] = source_dir

    with tenzir_node_endpoint(test, coverage=coverage) as endpoint:
        if not endpoint:
            return False
        try:
            unoptimized = subprocess.run(
                [TENZIR_BINARY, a, f"--endpoint={endpoint}", "-f", str(test)],
                timeout=test_config["timeout"],
                stdout=subprocess.PIPE,
                env=env,
            )

            # Update profile path for the optimized run
            if coverage:
                profile_path = os.path.join(coverage_dir, f"{test_name}-opt-%p.profraw")
                env["LLVM_PROFILE_FILE"] = profile_path

            optimized = subprocess.run(
                [TENZIR_BINARY, b, f"--endpoint={endpoint}", "-f", str(test)],
                timeout=test_config["timeout"],
                stdout=subprocess.PIPE,
                env=env,
            )
            diff = list(
                difflib.diff_bytes(
                    difflib.unified_diff,
                    unoptimized.stdout.splitlines(keepends=True),
                    optimized.stdout.splitlines(keepends=True),
                    n=2**31 - 1,
                )
            )[3:]
            if diff:
                diff = b"".join(diff)
            else:
                diff = b"".join(
                    map(
                        lambda x: b" " + x, unoptimized.stdout.splitlines(keepends=True)
                    )
                )
            ref_path = test.with_suffix(".diff")
            if update:
                ref_path.write_bytes(diff)
            else:
                expected = ref_path.read_bytes()
                if diff != expected:
                    report_failure(test, "")
                    print_diff(expected, diff, ref_path)
                    return False
            success(test)
            return True
        except Exception as e:
            report_failure(
                test, f"└─▶ \033[31mFailed to run node diff test: {e}\033[0m"
            )
            return False


@contextmanager
def check_server():
    stop = False
    port = None

    def server():
        counter = 0
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", 0))
        s.listen(100)
        s.settimeout(0.5)
        # TODO: Proper synchronization.
        nonlocal port
        port = s.getsockname()[1]
        while True:
            try:
                c, _ = s.accept()
                c.send(str(counter).encode())
                counter += 1
                c.close()
            except socket.timeout:
                if stop:
                    break

    t = threading.Thread(target=server)
    t.start()
    # TODO: Wait until it is actually started.
    while port is None:
        time.sleep(0.1)
    try:
        yield port
    finally:
        stop = True
        t.join()


class CustomFixture(ExtRunner):
    def __init__(self):
        super().__init__(prefix="custom", ext="sh")

    @override
    def run(self, test: Path, update: bool) -> bool:
        env = os.environ.copy()
        env["PATH"] = (ROOT / "_custom").as_posix() + ":" + env["PATH"]
        # TODO: Choose a random free port instead.
        with check_server() as port:
            env["TENZIR_TESTER_CHECK_PORT"] = str(port)
            env["TENZIR_TESTER_CHECK_UPDATE"] = str(int(update))
            env["TENZIR_TESTER_CHECK_PATH"] = str(test)
            try:
                cmd = ["sh", "-eu", str(test)]
                output = subprocess.check_output(
                    [x for x in cmd if x is not None], stderr=subprocess.PIPE, env=env
                )
                # TODO: What do to with output?
            except subprocess.CalledProcessError as e:
                with stdout_lock:
                    fail(test)
                    sys.stdout.buffer.write(e.stdout)
                    sys.stdout.buffer.write(e.output)
                    sys.stdout.buffer.write(e.stderr)
                return False
        success(test)
        return True


class CustomPythonFixture(ExtRunner):
    def __init__(self):
        super().__init__(prefix="python", ext="py")

    @override
    def run(self, test: Path, update: bool, coverage: bool) -> bool:
        test_config = parse_test_config(test, coverage=coverage)
        try:
            cmd = [sys.executable, str(test)]
            completed = subprocess.run(
                cmd,
                timeout=test_config["timeout"],
                # capture_output=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                env={
                    "PYTHONPATH": os.path.dirname(os.path.realpath(__file__)),
                    "TENZIR_PYTHON_FIXTURE_BINARY": TENZIR_BINARY,
                },
            )
            ref_path = test.with_suffix(".txt")
            if completed.returncode != 0:
                fail(test)
                return False
            if update:
                with open(ref_path, "wb") as f:
                    f.write(completed.stdout)
                    f.write(completed.stderr)
        except subprocess.CalledProcessError as e:
            with stdout_lock:
                fail(test)
                sys.stdout.buffer.write(e.stdout)
                sys.stdout.buffer.write(e.output)
                sys.stdout.buffer.write(e.stderr)
            return False
        success(test)
        return True


class CustomPythonFixtureWithNode(ExtRunner):
    def __init__(self):
        super().__init__(prefix="python_node", ext="py")

    @override
    def run(self, test: Path, update: bool, coverage: bool) -> bool:
        test_config = parse_test_config(test, coverage=coverage)
        with tenzir_node_endpoint(test, coverage=coverage) as endpoint:
            try:
                cmd = [sys.executable, str(test)]
                completed = subprocess.run(
                    cmd,
                    timeout=test_config["timeout"],
                    # capture_output=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                    env={
                        "PYTHONPATH": os.path.dirname(os.path.realpath(__file__)),
                        "TENZIR_PYTHON_FIXTURE_BINARY": TENZIR_BINARY,
                        "TENZIR_PYTHON_FIXTURE_ENDPOINT": endpoint,
                        "TENZIR_PYTHON_FIXTURE_TIMEOUT": str(test_config["timeout"]),
                    },
                )
                ref_path = test.with_suffix(".txt")
                if completed.returncode != 0:
                    fail(test)
                    return False
                if update:
                    with open(ref_path, "wb") as f:
                        f.write(completed.stdout)
                        f.write(completed.stderr)
            except subprocess.CalledProcessError as e:
                with stdout_lock:
                    fail(test)
                    sys.stdout.buffer.write(e.stdout)
                    sys.stdout.buffer.write(e.output)
                    sys.stdout.buffer.write(e.stderr)
                return False
        success(test)
        return True


RUNNERS = [
    AstRunner(),
    CustomFixture(),
    CustomPythonFixture(),
    CustomPythonFixtureWithNode(),
    ExecRunner(),
    FinalizeRunner(),
    InstantiationRunner(),
    IrRunner(),
    LexerRunner(),
    OldIrRunner(),
    OptRunner(),
]

runners = {}
for runner in RUNNERS:
    runners[runner.prefix] = runner

allowed_extensions = set()
for runner in RUNNERS:
    allowed_extensions.add(runner._ext)


class Worker:
    def __init__(self, queue, *, update: bool, coverage: bool = False):
        self._queue = queue
        self._result = None
        self._exception = None
        self._update = update
        self._coverage = coverage
        self._thread = threading.Thread(target=self._work)

    def start(self):
        self._thread.start()

    def join(self) -> Summary:
        self._thread.join()
        if self._exception:
            raise self._exception
        assert self._result is not None
        return self._result

    def _work(self) -> Summary:
        try:
            self._result = Summary()
            while True:
                try:
                    item = self._queue.pop()
                except IndexError:
                    break

                runner, test_path = item
                result = runner.run(test_path, self._update, self._coverage)
                self._result.total += 1
                if result == "skipped":
                    self._result.skipped += 1
                elif not result:
                    self._result.failed += 1
            return self._result
        except Exception as e:
            self._exception = e
            return self._result if self._result is not None else Summary()


def get_runner_for_test(test_path):
    """Determine the appropriate runner for a test based on its configuration."""
    config = parse_test_config(test_path)
    runner_name = config["test"]
    if runner_name in runners:
        return runners[runner_name]
    # Runner not found - this should never happen because parse_test_config should validate
    raise ValueError(f"Runner '{runner_name}' not found - this is a bug")


def collect_all_tests(dir: Path) -> Generator[Path, None, None]:
    for ext in allowed_extensions:
        yield from dir.glob(f"**/*.{ext}")


def main() -> None:
    # Parse arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("tests", nargs="*", type=Path, default=[ROOT])
    parser.add_argument("-u", "--update", action="store_true")
    parser.add_argument("--purge", action="store_true")
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Enable code coverage collection (increases timeouts by 5x)",
    )
    parser.add_argument(
        "--coverage-source-dir",
        type=str,
        help="Source directory for code coverage path mapping (defaults to current directory)",
    )
    default_jobs = 4 * (os.cpu_count() or 16)
    parser.add_argument("-j", "--jobs", type=int, default=default_jobs, metavar="N")
    args = parser.parse_args()
    if args.purge:
        for _, runner in runners.items():
            runner.purge()
        return

    # TODO Make sure that all tests are located in the `tests` directory.
    tests = [test for test in args.tests]
    todo = set()
    for test in tests:
        if test.resolve() == ROOT:
            # Collect all tests in all directories
            all_tests = []
            for dir_path in ROOT.iterdir():
                if dir_path.is_dir() and not dir_path.name.startswith("."):
                    all_tests.extend(list(collect_all_tests(dir_path)))

            # Process each test file using its configuration
            for test_path in all_tests:
                try:
                    runner = get_runner_for_test(test_path)
                    todo.add((runner, test_path))
                except ValueError as e:
                    # Show the error and exit
                    sys.exit(f"error: {e}")
            continue

        resolved = test.resolve()
        if not resolved.exists():
            sys.exit(f"error: test path `{test}` does not exist")

        # If it's a directory, collect all tests in it
        if resolved.is_dir():
            # Look for TQL files and use their config
            tql_files = list(collect_all_tests(resolved))
            if not tql_files:
                sys.exit(f"error: no {allowed_extensions} files found in {resolved}")

            for file_path in tql_files:
                try:
                    runner = get_runner_for_test(file_path)
                    todo.add((runner, file_path))
                except ValueError as e:
                    sys.exit(f"error: {e}")
        # If it's a file, determine the runner from its configuration
        elif resolved.is_file():
            if resolved.suffix[1:] in allowed_extensions:
                try:
                    runner = get_runner_for_test(resolved)
                    todo.add((runner, resolved))
                except ValueError as e:
                    sys.exit(f"error: {e}")
            else:
                # Error for non-TQL files
                sys.exit(
                    f"error: unsupported file type {resolved.suffix} for {resolved} - only {allowed_extensions} files are supported"
                )
        else:
            sys.exit(f"error: `{test}` is neither a file nor a directory")

    queue = list(todo)
    # Sort by test path (item[1])
    queue.sort(key=lambda tup: str(tup[1]), reverse=True)
    os.environ["TENZIR_EXEC__DUMP_DIAGNOSTICS"] = "true"
    if not TENZIR_BINARY:
        sys.exit(f"error: could not find TENZIR_BINARY executable `{TENZIR_BINARY}`")
    try:
        version = get_version()
    except FileNotFoundError:
        sys.exit(f"error: could not find TENZIR_BINARY executable `{TENZIR_BINARY}`")

    print(f"{INFO} running {len(queue)} tests with v{version}")

    # Pass coverage flag to workers
    workers = [
        Worker(queue, update=args.update, coverage=args.coverage)
        for _ in range(args.jobs)
    ]
    summary = Summary()
    for worker in workers:
        worker.start()
    for worker in workers:
        summary += worker.join()
    print(
        f"{INFO} {summary.total - summary.failed - summary.skipped}/{summary.total} tests passed ({summary.skipped} skipped)"
    )
    if args.coverage:
        coverage_dir = os.environ.get(
            "CMAKE_COVERAGE_OUTPUT_DIRECTORY", os.path.join(os.getcwd(), "coverage")
        )
        source_dir = args.coverage_source_dir or os.getcwd()
        print(f"{INFO} Code coverage data collected in {coverage_dir}")
        print(f"{INFO} Source directory for coverage mapping: {source_dir}")
    if summary.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
