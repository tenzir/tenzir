#!/usr/bin/env python3

from pathlib import Path
import argparse
import dataclasses
import os
import shutil
import subprocess
import sys
import threading
import difflib
import typing
import re
import time
import socket
import tempfile
from abc import ABC, abstractmethod
import builtins

from contextlib import contextmanager


TENZIR_BINARY = shutil.which("tenzir")
TENZIR_NODE_BINARY = shutil.which("tenzir-node")
ROOT = Path(os.path.dirname(__file__ or ".")).resolve()
INPUTS_DIR = ROOT / "inputs"
CHECKMARK = "\033[92;1m✓\033[0m"
CROSS = "\033[31m✘\033[0m"
INFO = "\033[94;1mi\033[0m"
TIMEOUT = 10

stdout_lock = threading.RLock()


def print(*args, **kwargs):
    # TODO: Properly solve the synchronization below.
    if "flush" not in kwargs:
        kwargs["flush"] = True
    return builtins.print(*args, **kwargs)


@dataclasses.dataclass
class Summary:
    failed: int = 0
    total: int = 0

    def __add__(self, other: "Summary") -> "Summary":
        return Summary(self.failed + other.failed, self.total + other.total)


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


def run_simple_test(
    test: Path, *, update: bool, args: typing.Sequence[str] = (), ext: str
) -> bool:
    try:
        # Check if tenzir.yaml exists in the same directory as the test
        config_file = test.parent / "tenzir.yaml"
        config_args = [f"--config={config_file}"] if config_file.exists() else []

        # Set up environment with INPUTS variable
        env = os.environ.copy()
        env["INPUTS"] = str(INPUTS_DIR)

        completed = subprocess.run(
            [
                TENZIR_BINARY,
                "--bare-mode",
                "--console-verbosity=warning",
                "--multi",
                *config_args,
                *args,
                "-f",
                test,
            ],
            timeout=TIMEOUT,
            stdout=subprocess.PIPE,
            env=env,
        )
        output = completed.stdout
        output = output.replace(bytes(ROOT) + b"/", b"")
        good = completed.returncode == 0
    except subprocess.TimeoutExpired:
        fail(test)
        return False
    except subprocess.CalledProcessError as e:
        with stdout_lock:
            fail(test)
            print(f'└─▶ \033[31msubprocess error "{e}":\033[0m')
        return False
    if test.read_bytes().startswith(b"// error") == good:
        with stdout_lock:
            fail(test)
            print(f"┌─▶ \033[31mgot unexpected exit code {completed.returncode}\033[0m")
            for last, line in last_and(output.split(b"\n")):
                prefix = "│ " if not last else "└─"
                sys.stdout.buffer.write(prefix.encode() + line + b"\n")
        return False
    if not good:
        ext = "txt"
    ref_path = test.with_suffix(f".{ext}")
    if update:
        with ref_path.open("wb") as f:
            f.write(output)
    else:
        if not ref_path.exists():
            with stdout_lock:
                fail(test)
                print(f'└─▶ \033[31mFailed to find ref file: "{ref_path}"\033[0m')
            return False
        expected = ref_path.read_bytes()
        if expected != output:
            with stdout_lock:
                fail(test)
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
    def run(self, test_name: str, update: bool) -> bool:
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
    def __init__(self, *, prefix: str):
        super().__init__(prefix=prefix, ext="tql")


class AstRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="ast")

    def run(self, test: str, update: bool) -> bool:
        return run_simple_test(test, update=update, args=("--dump-ast",), ext="txt")


class OldIrRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="oldir")

    def run(self, test: str, update: bool) -> bool:
        return run_simple_test(
            test, update=update, args=("--dump-pipeline",), ext="txt"
        )


class IrRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="ir")

    def run(self, test: str, update: bool) -> bool:
        return run_simple_test(test, update=update, args=("--dump-ir",), ext="txt")


class DiffRunner(TqlRunner):
    def __init__(self, *, a: str, b: str, prefix: str):
        super().__init__(prefix=prefix)
        self._a = a
        self._b = b

    def run(self, test: Path, update: bool) -> bool:
        # Set up environment with INPUTS variable
        env = os.environ.copy()
        env["INPUTS"] = str(INPUTS_DIR)

        unoptimized = subprocess.run(
            [TENZIR_BINARY, self._a, "-f", test],
            timeout=TIMEOUT,
            stdout=subprocess.PIPE,
            env=env,
        )
        optimized = subprocess.run(
            [TENZIR_BINARY, self._b, "-f", test],
            timeout=TIMEOUT,
            stdout=subprocess.PIPE,
            env=env,
        )
        diff = list(
            difflib.diff_bytes(
                difflib.unified_diff,
                unoptimized.stdout.splitlines(keepends=True),
                optimized.stdout.splitlines(keepends=True),
                n=float("inf"),
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
                with stdout_lock:
                    fail(test)
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

    def run(self, test: str, update: bool) -> bool:
        return run_simple_test(
            test, update=update, args=("--dump-finalized",), ext="txt"
        )


class ExecRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="exec")

    def run(self, test: str, update: bool) -> bool:
        return run_simple_test(test, update=update, ext="txt")


class NodeRunner(TqlRunner):
    def __init__(self):
        super().__init__(prefix="node")

    def run(self, test: Path, update: bool) -> bool:
        # Check if tenzir-node binary is available
        if not TENZIR_NODE_BINARY:
            with stdout_lock:
                fail(test)
                print(f"└─▶ \033[31mCould not find tenzir-node binary\033[0m")
            return False

        # Start tenzir-node process
        node_process = None
        temp_dir = None
        try:
            # Create a temporary directory for the node data
            temp_dir = tempfile.TemporaryDirectory()

            # Check if tenzir.yaml exists in the same directory as the test
            config_file = test.parent / "tenzir.yaml"
            config_args = [f"--config={config_file}"] if config_file.exists() else []

            # Start tenzir-node with dynamic port allocation
            # Set up environment with INPUTS variable
            env = os.environ.copy()
            env["INPUTS"] = str(INPUTS_DIR)

            node_process = subprocess.Popen(
                [
                    TENZIR_NODE_BINARY,
                    "--bare-mode",
                    "--console-verbosity=warning",
                    f"--state-directory={temp_dir.name}",
                    "--endpoint=localhost:0",
                    "--print-endpoint",
                    *config_args,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=env,
            )

            # Wait for the node to print its endpoint and parse it
            endpoint = None
            for line in node_process.stdout:
                endpoint = line.strip()
                break

            if not endpoint:
                with stdout_lock:
                    fail(test)
                    print(f"└─▶ \033[31mFailed to get endpoint from tenzir-node")
                return False

            # Run the test against the node with the discovered endpoint
            result = run_simple_test(
                test, update=update, args=[f"--endpoint={endpoint}"], ext="txt"
            )

            return result
        except Exception as e:
            with stdout_lock:
                fail(test)
                print(f"└─▶ \033[31mFailed to run node test: {e}\033[0m")
            return False
        finally:
            # Make sure we always terminate the node process
            if node_process:
                try:
                    node_process.terminate()
                    try:
                        node_process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        # If termination times out, force kill the process
                        node_process.kill()
                        node_process.wait()
                except Exception as e:
                    # Ensure we don't fail the test on process termination issues
                    with stdout_lock:
                        print(f"└─▶ \033[31mError terminating node process: {e}")

            # Clean up temp directory
            if temp_dir:
                try:
                    temp_dir.cleanup()
                except Exception as e:
                    # Ensure we don't fail the test on cleanup issues
                    with stdout_lock:
                        print(f"└─▶ \033[31mError cleaning up temp directory: {e}")


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

    def run(self, path: Path, update: bool) -> bool:
        env = os.environ.copy()
        env["PATH"] = (ROOT / "_custom").as_posix() + ":" + env["PATH"]
        # TODO: Choose a random free port instead.
        with check_server() as port:
            env["TENZIR_TESTER_CHECK_PORT"] = str(port)
            env["TENZIR_TESTER_CHECK_UPDATE"] = str(int(update))
            env["TENZIR_TESTER_CHECK_PATH"] = str(path)
            try:
                output = subprocess.check_output(
                    ["sh", "-eu", path], stderr=subprocess.PIPE, env=env
                )
                # TODO: What do to with output?
            except subprocess.CalledProcessError as e:
                with stdout_lock:
                    fail(path)
                    sys.stdout.buffer.write(e.stdout)
                    sys.stdout.buffer.write(e.output)
                    sys.stdout.buffer.write(e.stderr)
                return False
        success(path)
        return True


RUNNERS = [
    AstRunner(),
    ExecRunner(),
    NodeRunner(),
    CustomFixture(),
    OldIrRunner(),
    IrRunner(),
    InstantiationRunner(),
    OptRunner(),
    FinalizeRunner(),
]

runners = {}
for runner in RUNNERS:
    runners[runner.prefix] = runner


class Worker:
    def __init__(self, queue, *, update: bool):
        self._queue = queue
        self._result = None
        self._exception = None
        self._update = update
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
                success = item[0].run(item[1], self._update)
                self._result.total += 1
                if not success:
                    self._result.failed += 1
        except Exception as e:
            self._exception = e


def main() -> None:
    # Parse arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("tests", nargs="*", type=Path, default=[ROOT])
    parser.add_argument("-u", "--update", action="store_true")
    parser.add_argument("--purge", action="store_true")
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
            for name, runner in runners.items():
                todo.update(runner.collect_tests(ROOT / name))
            continue
        resolved = test.resolve()
        try:
            target_name = resolved.relative_to(ROOT).parts[0]
        except ValueError:
            if not test.is_absolute():
                resolved = ROOT / test
                target_name = resolved.relative_to(ROOT).parts[0]
            else:
                sys.exit(f"error: `{test}` is not in `{ROOT}`")
        for name, runner in runners.items():
            if target_name == name:
                todo.update(runner.collect_tests(resolved))
                break
        else:
            sys.exit(f"error: no runner found for `{test}`")

    queue = list(todo)
    queue.sort(key=lambda tup: tup[1], reverse=True)
    os.environ["TENZIR_EXEC__DUMP_DIAGNOSTICS"] = "true"
    try:
        version = get_version()
    except FileNotFoundError:
        sys.exit(f"error: could not find `{TENZIR_BINARY}` executable")

    # Print warning if tenzir-node binary is not found
    if "node" in [item[0].prefix for item in queue] and not TENZIR_NODE_BINARY:
        print(
            f"{INFO} warning: could not find `tenzir-node` executable, node tests will be skipped"
        )

    print(f"{INFO} running {len(queue)} tests with v{version}")

    workers = [Worker(queue, update=args.update) for _ in range(args.jobs)]
    summary = Summary()
    for worker in workers:
        worker.start()
    for worker in workers:
        summary += worker.join()
    print(f"{INFO} {summary.total - summary.failed}/{summary.total} tests passed")
    if summary.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
