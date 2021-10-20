#!/usr/bin/env python
"""tenzir core / vast integration test script v1

"""

import argparse
import difflib
import filecmp
import gzip
import itertools
import json
import logging
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from contextlib import suppress
from datetime import datetime
from enum import Enum
from pathlib import Path
from string import Template
from typing import Callable, List, NamedTuple, Optional, TypeVar, Union

import coloredlogs
import packages.wait as wait
import schema
import yaml

LOGGER = logging.getLogger("VAST")
VAST_PORT = 42024
STEP_TIMEOUT = 30
CURRENT_SUBPROCS: List[subprocess.Popen] = []
SET_DIR = Path()


class Result(Enum):
    SUCCESS = 1  # Baseline comparison succeded.
    FAILURE = 2  # Baseline mismatch.
    ERROR = 3  # Crashes or returns with non-zero exit code.
    TIMEOUT = 4  # Command timed out


class Fixture(NamedTuple):
    enter: str
    exit: str


class Step(NamedTuple):
    command: List[str]
    input: Optional[Path]
    transformation: Optional[str]
    expected_result: Optional[Result]


class Condition(NamedTuple):
    subcommand: str


class Test(NamedTuple):
    tags: Optional[str]
    condition: Optional[Condition]
    config_file: Optional[str]
    fixture: Optional[str]
    steps: List[Union[Step, Condition]]


def signal_subprocs(signum):
    """send signal recieved to subprocesses"""
    for proc in reversed(CURRENT_SUBPROCS):
        if proc.poll() is None:
            LOGGER.debug(f"sending signal {signum} to {proc.args}")
            proc.send_signal(signum)


def handle_exit_signal(signum, _frame):
    """send signal recieved to subprocesses and exit"""
    LOGGER.warning(f"got signal {signum}, shutting down")
    signal_subprocs(signum)
    sys.exit(1)


def timeout_handler(_signum, frame):
    """Terminate subprocs and raise"""
    signal_subprocs(signal.SIGTERM)
    raise OSError("Timeout reached")


signal.signal(signal.SIGINT, handle_exit_signal)
signal.signal(signal.SIGTERM, handle_exit_signal)
signal.signal(signal.SIGALRM, timeout_handler)

# TODO tobim: check if asyncio would be a better approach
def spawn(*popenargs, **kwargs):
    """Helper function around the Popen constructor
    that puts the created process into a registry
    """
    proc = subprocess.Popen(*popenargs, **kwargs)
    CURRENT_SUBPROCS.append(proc)
    return proc


def try_wait(process, timeout, expected_result):
    """Wait for a specified time or terminate the process"""
    try:
        # Ignore SIGPIPE errors
        if process.wait(timeout) not in [0, -13]:
            log = LOGGER.error
            if expected_result == Result.ERROR:
                log = LOGGER.debug
            log(f"{process.args} returned {process.returncode}")
            return Result.ERROR
        return Result.SUCCESS
    except subprocess.TimeoutExpired:
        if expected_result == Result.TIMEOUT:
            LOGGER.debug(f"expected timeout reached, terminating process")
        else:
            LOGGER.error(f"timeout reached, terminating process")
        process.terminate()
        return Result.TIMEOUT


def run_flamegraph(args, svg_file):
    """Perform instrumentation and produce an output svg"""
    LOGGER.debug(f"writing flamegraph {args} to {svg_file}")
    flamegraph = Path(args.flamegraph_path).resolve()
    with open(svg_file, "w") as svg:
        # refresh sudo credential cache in a blocking call to be sure to catch
        # the entire test run
        subprocess.run(["sudo", "-v"])
        return spawn(
            [flamegraph, "-s", str(args.timeout)], stdout=svg, cwd=args.directory
        )


def now():
    # return time.clock_gettime(time.CLOCK_MONOTONIC)
    return time.process_time()


class TestSummary:
    """Stats keeper"""

    def __init__(self, step_count):
        self.step_count = step_count
        self.unexpected_results = 0
        self.succeeded = 0
        self.failed = 0
        self.errors = 0
        self.timeouts = 0

    def __repr__(self):
        return f"({self.step_count}/{self.succeeded}/{self.failed})"

    def count(self, result, expected_result):
        """Count step result"""
        if result is Result.SUCCESS:
            self.succeeded += 1
        elif result is Result.FAILURE:
            self.failed += 1
        elif result is Result.ERROR:
            self.errors += 1
        elif result is Result.TIMEOUT:
            self.timeouts += 1
        else:
            pass
        if result != expected_result:
            self.unexpected_results += 1

    def dominant_state(self):
        """Reduce to a single Result"""
        if self.errors > 0:
            return Result.ERROR
        if self.timeouts > 0:
            return Result.TIMEOUT
        if self.failed > 0:
            return Result.FAILURE
        return Result.SUCCESS

    def successful(self):
        return self.unexpected_results == 0


def empty(iterable):
    try:
        first = next(iterable)
    except StopIteration:
        return True
    return False


def is_non_deterministic(command):
    positionals = list(filter(lambda x: x[0] != "-", command))
    return positionals[0] in {"export", "explore", "get", "pivot"}


def run_step(
    basecmd, step_id, step, work_dir, baseline_dir, update_baseline, expected_result
):
    try:
        stdout = work_dir / f"{step_id}.out"
        stderr = work_dir / f"{step_id}.err"
        cmd = basecmd + step.command
        info_string = " ".join(map(str, cmd))
        client = spawn(
            basecmd + step.command,
            stdin=(subprocess.PIPE if step.input else None),
            stdout=open(stdout, "w+"),
            stderr=open(stderr, "w"),
            cwd=work_dir,
        )
        start_time = now()
        # Invoking process.
        if step.input:
            incmd = []
            if str(step.input).endswith("gz"):
                incmd += ["gunzip", "-c", str(step.input)]
            else:
                incmd += ["cat", str(step.input)]
            info_string = " ".join(incmd) + " | " + info_string
            input_p = spawn(incmd, stdout=client.stdin)
            result = try_wait(
                input_p,
                timeout=STEP_TIMEOUT - (now() - start_time),
                expected_result=expected_result,
            )
            client.stdin.close()
        result = try_wait(
            client,
            timeout=STEP_TIMEOUT - (now() - start_time),
            expected_result=expected_result,
        )
        if result is Result.ERROR and result != expected_result:
            LOGGER.warning("standard error:")
            for line in open(stderr).readlines()[-100:]:
                LOGGER.warning(f"    {line}")
            return result
        # Perform baseline update or comparison.
        baseline = baseline_dir / f"{step_id}.ref"
        sort_output = is_non_deterministic(step.command)
        if update_baseline:
            LOGGER.debug("updating baseline")
            if not baseline_dir.exists():
                baseline_dir.mkdir(parents=True)
        else:
            LOGGER.debug("comparing step output to baseline")
        with open(stdout) as out_handle:
            out = None
            if step.transformation:
                LOGGER.debug(f"transforming output with `{step.transformation}`")
                try:
                    out = subprocess.run(
                        [step.transformation],
                        stdin=out_handle,
                        stdout=subprocess.PIPE,
                        timeout=STEP_TIMEOUT,
                        shell=True,
                    ).stdout.decode("utf8")
                except subprocess.TimeoutExpired:
                    LOGGER.error(f"timeout reached, terminating transformation")
                    return Result.TIMEOUT
            else:
                out = out_handle.read()
            diff = None
            output_lines = out.splitlines(keepends=True)
            if sort_output:
                output_lines = sorted(output_lines)
            if update_baseline:
                with open(baseline, "w") as ref_handle:
                    for line in output_lines:
                        ref_handle.write(line)
                return Result.SUCCESS
            baseline_lines = []
            if baseline.exists():
                baseline_lines = open(baseline).readlines()
            diff = difflib.unified_diff(
                baseline_lines,
                output_lines,
                fromfile=str(baseline),
                tofile=str(stdout),
            )
            delta = list(diff)
            if delta:
                if expected_result != Result.FAILURE:
                    LOGGER.warning("baseline comparison failed")
                    sys.stdout.writelines(delta)
                return Result.FAILURE
    except subprocess.CalledProcessError as err:
        if expected_result != Result.ERROR:
            LOGGER.error(err)
        return Result.ERROR
    return result


class Server:
    """Server fixture implementation details"""

    def __init__(
        self,
        app,
        args,
        work_dir,
        name="node",
        port=VAST_PORT,
        config_file=None,
        **kwargs,
    ):
        self.app = app
        if config_file:
            self.config_arg = f"--config={SET_DIR/config_file}"
        else:
            self.config_arg = None
        self.name = name
        self.cwd = work_dir / self.name
        self.port = port
        command = [self.app, "--bare-mode"]
        if self.config_arg:
            command.append(self.config_arg)
        command = command + args
        LOGGER.debug(f"starting server fixture: {command}")
        LOGGER.debug(f"waiting for port {self.port} to be available")
        if not wait.tcp.closed(self.port, timeout=5):
            raise RuntimeError("Port is blocked by another process.\nAborting tests...")
        self.cwd.mkdir(parents=True)
        out = open(self.cwd / "out", "w")
        err = open(self.cwd / "err", "w")
        self.process = spawn(
            command,
            cwd=self.cwd,
            stdout=out,
            stderr=err,
            **kwargs,
        )
        LOGGER.debug(f"waiting for server to listen on port {self.port}")
        if not wait.tcp.open(self.port, timeout=10):
            raise RuntimeError("Server could not aquire port.\nAborting tests")

    def stop(self):
        """Stops the server"""
        command = [self.app, "--bare-mode"]
        if self.config_arg:
            command.append(self.config_arg)
        command = command + ["-e", f":{self.port}", "stop"]
        LOGGER.debug(f"stopping server fixture: {command}")
        stop_out = open(self.cwd / "stop.out", "w")
        stop_err = open(self.cwd / "stop.err", "w")
        stop = 0
        try:
            stop = spawn(
                command,
                cwd=self.cwd,
                stdout=stop_out,
                stderr=stop_err,
            ).wait(STEP_TIMEOUT)
        except:
            stop.kill()
        try:
            self.process.wait(STEP_TIMEOUT)
        except:
            self.process.kill()


class Tester:
    """Test runner"""

    def __init__(self, args, fixtures, config_file):
        self.args = args
        self.app = args.app
        self.cmd = Path(args.app).resolve()
        self.config_file = config_file
        self.fixtures = fixtures
        self.test_dir = args.directory
        self.update = args.update

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with suppress(OSError):
            self.test_dir.rmdir()

    def check_condition(self, condition):
        check = spawn(self.app + " " + condition.subcommand, shell=True)
        ret = check.wait(STEP_TIMEOUT)
        return ret

    def check_skip(self, test):
        """Checks if a test should run if a condition is defined"""
        if test.condition:
            return self.check_condition(test.condition)
        return False

    def check_guards(self, steps):
        result = []
        for step in steps:
            if isinstance(step, Condition):
                if self.check_condition(step):
                    LOGGER.debug("guard condition false, skipping steps")
                    return result
            else:
                result.append(step)
        return result

    def run(self, test_name, test):
        """Runs a single test"""
        LOGGER.info(f"running test: {test_name}")
        normalized_test_name = test_name.replace(" ", "-").lower()
        baseline_dir = self.args.set.parent / "reference" / normalized_test_name
        work_dir = self.test_dir / normalized_test_name
        if work_dir.exists():
            LOGGER.debug(f"removing existing work directory {work_dir}")
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True)
        summary = TestSummary(len(test.steps))
        step_i = 0
        # Locate fixture.
        dummy_fixture = Fixture("pass", "pass")
        fixture = dummy_fixture if not test.fixture else self.fixtures.get(test.fixture)
        cmd = [self.cmd, "--bare-mode"]
        if test.config_file:
            cmd.append(f"--config={test.config_file}")
        elif self.config_file:
            cmd.append(f"--config={self.config_file}")
        fenter = Template(fixture.enter).substitute(locals())
        fexit = Template(fixture.exit).substitute(locals())
        # Invoke test.
        exec(fenter)
        if self.args.flamegraph:
            svg_file = work_dir / f"{normalized_test_name}.svg"
            run_flamegraph(self.args, svg_file)
        for step in test.steps:
            step_id = "step_{:02d}".format(step_i)
            LOGGER.debug(f"running step {step_i}: {step.command}")
            result = run_step(
                cmd,
                step_id,
                step,
                work_dir,
                baseline_dir,
                self.update,
                step.expected_result,
            )
            summary.count(result, step.expected_result)
            if not self.args.keep_going and result != step.expected_result:
                LOGGER.warning("skipping remaining steps after error")
                break
            step_i += 1
        exec(fexit)
        # Summarize result.
        if summary.successful():
            LOGGER.info(f"ran all {summary.step_count} steps successfully")
            if not self.args.keep:
                LOGGER.debug(f"removing working directory {work_dir}")
                shutil.rmtree(work_dir)
            return Result.SUCCESS
        LOGGER.warning(
            f"ran {summary.succeeded}/{summary.step_count} steps successfully"
        )
        return summary.dominant_state()


def validate(data):
    def is_file(path):
        return path.is_file()

    def to_fixture(data):
        return Fixture(**data)

    def to_step(data):
        return Step(**data)

    def guard_to_condition(guard):
        return Condition(guard["guard"])

    def to_test(data):
        data["config_file"] = data.pop("config-file")
        return Test(**data)

    def absolute_path(path):
        absolute = Path(os.path.expanduser(path))
        if not absolute.is_absolute():
            absolute = (SET_DIR / path).resolve()
        return absolute

    def replace_path(raw_command):
        return raw_command.replace("@.", str(SET_DIR))

    def to_command(raw_command):
        return shlex.split(replace_path(raw_command))

    def to_result(raw_result):
        return Result[raw_result.upper()]

    fixture = schema.Schema(
        schema.And(
            {"enter": schema.And(str, len), "exit": schema.And(str, len)},
            schema.Use(to_fixture),
        )
    )
    fixtures = schema.Schema({schema.And(str, len): fixture})
    step = schema.Schema(
        schema.And(
            {
                "command": schema.And(
                    schema.Const(schema.And(str, len)), schema.Use(to_command)
                ),
                schema.Optional("input", default=None): schema.And(
                    schema.Use(absolute_path), is_file
                ),
                schema.Optional("transformation", default=None): schema.Use(
                    replace_path
                ),
                schema.Optional("expected_result", default=Result.SUCCESS): schema.Use(
                    to_result
                ),
            },
            schema.Use(to_step),
        )
    )
    guard = schema.Schema(schema.And({"guard": str}, schema.Use(guard_to_condition)))
    test = schema.Schema(
        schema.And(
            {
                schema.Optional("tags", default=None): [str],
                schema.Optional("condition", default=None): schema.Use(Condition),
                schema.Optional("config-file", default=None): schema.Use(absolute_path),
                schema.Optional("fixture", default=None): str,
                "steps": [schema.Or(step, guard)],
            },
            schema.Use(to_test),
        )
    )
    tests = schema.Schema({schema.And(str, len): test})
    sch = schema.Schema(
        {
            schema.Optional("config-file", default=None): schema.Use(absolute_path),
            schema.Optional("fixtures", default=None): fixtures,
            "tests": tests,
        }
    )
    return sch.validate(data)


def tagselect(tags, tests):
    if not tags:
        return {k: t for k, t in tests.items() if "disabled" not in t.tags}
    tagset = set(tags)
    return {k: t for k, t in tests.items() if tagset & set(t.tags)}


def run(args, test_dec):
    tests = test_dec["tests"]
    selected_tests = {}
    explicit_tests = {}
    if args.test:
        # Create a subset with only the submitted tests present as keys
        lower = {t.lower() for t in args.test}
        exists = lambda x: any(re.search(k, x.lower()) for k in lower)
        explicit_tests = {k: tests[k] for k in tests.keys() if exists(k)}
    if not args.test or args.tag:
        selected_tests = tagselect(args.tag, tests)
    tests = dict(selected_tests, **explicit_tests)
    try:
        with Tester(args, test_dec["fixtures"], test_dec["config-file"]) as tester:
            result = True
            for name, definition in tests.items():
                # Skip the test if the condition is not fulfilled
                if tester.check_skip(definition):
                    LOGGER.debug(f"skipping test {name}")
                    continue
                # Check and remove guards from the list of steps
                definition = definition._replace(
                    steps=tester.check_guards(definition.steps)
                )
                test_result = Result.FAILURE
                for i in range(0, args.repetitions):
                    test_result = tester.run(name, definition)
                    if test_result is Result.TIMEOUT:
                        if i < args.repetitions - 1:
                            # Try again.
                            LOGGER.warning(
                                f"Re-running test {name} {i+2}/{args.repetitions}"
                            )
                            continue
                    if test_result is not Result.SUCCESS:
                        result = False
                    # Only timeouts trigger repetitions.
                    break
            return result
    except Exception as err:
        signal_subprocs(signal.SIGTERM)
        raise err


def main():
    """The main function"""
    parser = argparse.ArgumentParser(
        description="Test runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--app", default="./core", help="Path to the executable (vast/core)"
    )
    parser.add_argument(
        "-s",
        "--set",
        type=Path,
        help="Run the testset from this test definition YAML file",
    )
    parser.add_argument(
        "-T", "--tag", nargs="+", help="The tag for which tests will be run"
    )
    parser.add_argument(
        "-t", "--test", nargs="+", help="The test(s) to run (runs all tests if unset)"
    )
    parser.add_argument(
        "-u", "--update", action="store_true", help="Update baseline for tests"
    )
    parser.add_argument(
        "-d",
        "--directory",
        default="run_<current_ISO_timestamp>",
        type=Path,
        help="The basedir for the test runs",
    )
    parser.add_argument(
        "-k",
        "--keep-going",
        action="store_true",
        help="Continue to evaluate in case of a failed test",
    )
    parser.add_argument(
        "-K", "--keep", action="store_true", help="Keep artifacts of successful runs"
    )
    parser.add_argument(
        "--timeout", type=int, default=0, help="Test timeout in seconds"
    )
    parser.add_argument(
        "-r",
        "--repetitions",
        default=3,
        help="Repeat count for tests that timed out",
    )
    parser.add_argument(
        "-l",
        "--list",
        nargs="*",
        help="Return a list of available tests optionally filtered with tags",
    )
    parser.add_argument(
        "-L",
        "--list-tags",
        action="store_true",
        help="Return a list of all available tags",
    )
    parser.add_argument(
        "--flamegraph",
        action="store_true",
        help="Generate a flamegraph of the test run",
    )
    parser.add_argument(
        "--flamegraph_path",
        default="scripts/flamegraph",
        type=Path,
        help="Path to flamegraph script",
    )
    parser.add_argument(
        "-v", "--verbosity", default="INFO", help="Set the logging verbosity"
    )
    args = parser.parse_args()
    # Setup logging.
    LOGGER.setLevel(args.verbosity)
    fmt = "%(asctime)s %(levelname)-8s %(message)s"
    colored_formatter = coloredlogs.ColoredFormatter(fmt)
    plain_formatter = logging.Formatter(fmt)
    formatter = colored_formatter if sys.stdout.isatty() else plain_formatter
    ch = logging.StreamHandler()
    ch.setLevel(args.verbosity)
    ch.setFormatter(formatter)
    LOGGER.addHandler(ch)
    # Create a new handler for log level CRITICAL.
    class ShutdownHandler(logging.Handler):
        def emit(self, record):
            logging.shutdown()
            signal_subprocs(signal.SIGTERM)
            sys.exit(1)

    # Register this handler with log level CRITICAL (which equals to 50).
    sh = ShutdownHandler(level=50)
    sh.setFormatter(formatter)
    LOGGER.addHandler(sh)
    # Load test set.
    if not args.set:
        args.set = Path(__file__).resolve().parent / "vast_integration_suite.yaml"
    args.set = args.set.resolve()
    LOGGER.debug(f"resolved test set path to {args.set}")
    global SET_DIR
    SET_DIR = args.set.parent
    test_file = open(args.set, "r")
    test_dict = yaml.full_load(test_file)
    test_dec = validate(test_dict)
    # Print tests.
    if args.list is not None:
        selection = tagselect(args.list, test_dec["tests"])
        for test in selection.keys():
            print(test)
        return
    # Print test tags.
    if args.list_tags:
        tags = set().union(*[set(t.tags) for _, t in test_dec["tests"].items()])
        for tag in tags:
            print(tag)
        return
    # Create working directory.
    if args.directory.name == "run_<current_ISO_timestamp>":
        timestamp = datetime.now().isoformat(timespec="seconds")
        args.directory = Path(f"run_{timestamp}")
    LOGGER.debug(f"keeping state in {args.directory}")
    if not args.directory.exists():
        args.directory.mkdir(parents=True)
    # Setup signal handlers and run.
    signal.alarm(args.timeout)
    success = run(args, test_dec)
    signal.alarm(0)
    with suppress(OSError):
        args.directory.rmdir()
    retcode = 0 if success else 1
    sys.exit(retcode)


if __name__ == "__main__":
    main()
