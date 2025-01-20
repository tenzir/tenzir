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

from contextlib import contextmanager


# TODO: This needs a better way to discover tenzir, if its not in the path
BINARY = shutil.which("tenzir") or "../../build/debug/bin/tenzir"
ROOT = Path(os.path.dirname(__file__ or ".")).resolve()
CHECKMARK = "\033[92;1m✓\033[0m"
CROSS = "\033[31m✘\033[0m"
INFO = "\033[94;1mi\033[0m"
TIMEOUT = 10

stdout_lock = threading.RLock()


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
                BINARY,
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


# TODO: This line number is incorrect. We need to go to the first `-` or `+`.
def get_line_number(line: bytes) -> int:
    return int(re.match(b"@@ -([0-9]*)", line).group(1).decode())


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


def run_simple_test(
    test: Path, *, update: bool, args: typing.Sequence[str] = (), ext: str
) -> bool:
    try:
        completed = subprocess.run(
            [BINARY, *args, "-f", test], timeout=TIMEOUT, stdout=subprocess.PIPE
        )
        output = completed.stdout
        output = output.replace(bytes(ROOT) + b"/", b"")
        good = completed.returncode == 0
    except subprocess.TimeoutExpired:
        fail(test)
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
            fail(test)
            return False
        expected = ref_path.read_bytes()
        if expected != output:
            diff = list(
                difflib.diff_bytes(
                    difflib.unified_diff,
                    expected.splitlines(keepends=True),
                    output.splitlines(keepends=True),
                    # b"expected",
                    # b"actual",
                    n=2,
                )
            )
            with stdout_lock:
                fail(test)
                skip = 2
                for i, line in enumerate(diff):
                    if skip > 0:
                        skip -= 1
                        continue
                    if line.startswith(b"@@"):
                        print(
                            f"┌─▶ \033[31m{ref_path.relative_to(ROOT)}:{get_line_number(line)}\033[0m"
                        )
                        continue
                    if line.startswith(b"+"):
                        line = b"\033[92m" + line + b"\033[0m"
                    elif line.startswith(b"-"):
                        line = b"\033[31m" + line + b"\033[0m"
                    prefix = ("│ " if i != len(diff) - 1 else "└─").encode()
                    sys.stdout.buffer.write(prefix + line)
            return False
    success(test)
    return True


class Fixture :
    def __init__(self, path_prefix:str ) :
        self.path_prefix = path_prefix

    def collect_with_ext(self, path: Path, ext: str) -> set:
        todo = set()
        if path.relative_to(ROOT).parts[0] != self.path_prefix:
            raise ValueError(
                f"test path `{path}` should` not be collected via fixture `{self.path_prefix}`"
            )
        if path.suffix == f".{ext}":
            todo.add((self, path))
            return todo
        for test in path.glob(f"**/*.{ext}"):
            todo.add((self, test))
        return todo

    def collect_tests(self, path: Path):
        return self.collect_with_ext(path, "tql")

    def purge(self) :
        purge_base = ROOT/self.path_prefix
        print(f"purging `{purge_base}`")
        for p in purge_base.rglob("*") :
            if p.is_dir() :
                continue
            if p.suffix == ".tql" :
                continue
            # p.unlink()

    def run(self, test_name: str, update: bool) -> bool:
        raise NotImplementedError


class AST_Fixture(Fixture):

    def run(self, test: str, update: bool) -> bool:
        return run_simple_test(test, update=update, args=("--dump-ast",), ext="txt")


class Exec_Fixture(Fixture) :

    def run(self, test: str, update: bool) -> bool:
        return run_simple_test(test, update=update, ext="json")


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


class CustomFixture(Fixture):
    def __init__(self):
        super().__init__("custom")

    def collect_tests(self, path: Path):
        return self.collect_with_ext(path, "sh")

    def run(self, path: Path, update: bool) -> bool:
        env = os.environ.copy()
        env["PATH"] = (ROOT / "_todo").as_posix() + ":" + env["PATH"]
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


fixtures = {
    "ast": AST_Fixture("ast"),
    "exec": Exec_Fixture("exec"),
    "custom": CustomFixture(),
}


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
    parser.add_argument("-p", "--purge", action="store_true")
    parser.add_argument("-j", "--jobs", type=int, default=16)
    args = parser.parse_args()
    #
    if args.purge:
        for name,fixture in fixtures.items() :
            fixture.purge()
        return

    # TODO Make sure that all tests are located in the `tests` directory.
    tests = [test.resolve() for test in args.tests]
    todo = set()
    for test in tests:
        if test == ROOT:
            for name,fixture in fixtures.items() :
                todo.update( fixture.collect_tests(ROOT/name) )
            continue
        found = False
        for name, fixture in fixtures :
            if test.parts[0] == name :
                todo.update( fixture.collect_tests(test) )
                found = True
        if not found :
            print(f"unknown category fixture")

    queue = list(todo)
    queue.sort(key=lambda tup: tup[1], reverse=True)

    # TODO
    os.environ["TENZIR_TQL2"] = "true"
    os.environ["TENZIR_EXEC__DUMP_DIAGNOSTICS"] = "true"
    try:
        version = get_version()
    except FileNotFoundError:
        sys.exit(f"could not find `{BINARY}` executable")
    print(f"{INFO} running {version}")

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
