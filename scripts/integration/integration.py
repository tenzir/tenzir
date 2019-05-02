#!/usr/bin/env python
"""tenzir core / vast integration test script v1

"""

import argparse
import coloredlogs
import difflib
import filecmp
import gzip
import itertools
import logging
import os
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
from typing import Callable, List, NamedTuple, Optional, TypeVar

import packages.wait as wait
import schema
import yaml

LOGGER = logging.getLogger('VAST')
VAST_PORT = 42024
STEP_TIMEOUT = 30
CURRENT_SUBPROCS: List[subprocess.Popen] = []

class Fixture(NamedTuple):
    enter: str
    exit: str

class Step(NamedTuple):
    command: List[str]
    input: Optional[Path]

class Test(NamedTuple):
    tags: Optional[str]
    condition: Optional[str]
    fixture: Optional[str]
    steps: List[Step]

def signal_subprocs(signum):
    """send signal recieved to subprocesses"""
    for proc in reversed(CURRENT_SUBPROCS):
        if proc.poll() is None:
            LOGGER.debug(f'sending signal {signum} to {proc.args}')
            proc.send_signal(signum)

def handle_exit_signal(signum, _frame):
    """send signal recieved to subprocesses and exit"""
    LOGGER.warning(f'got signal {signum}, shutting down')
    signal_subprocs(signum)
    sys.exit(1)

def timeout_handler(_signum, frame):
    """Terminate subprocs and raise"""
    signal_subprocs(signal.SIGTERM)
    raise OSError('Timeout reached')

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

def run_flamegraph(args, svg_file):
    """Perform instrumentation and produce an output svg"""
    LOGGER.debug(f'writing flamegraph {args} to {svg_file}')
    flamegraph = Path(args.flamegraph_path).resolve()
    with open(svg_file, 'w') as svg:
        # refresh sudo credential cache in a blocking call to be sure to catch
        # the entire test run
        subprocess.run(['sudo', '-v'])
        return spawn([flamegraph, '-s', str(args.timeout)],
                     stdout=svg,
                     cwd=args.directory)

def now():
    # return time.clock_gettime(time.CLOCK_MONOTONIC)
    return time.process_time()

class Result(Enum):
    SUCCESS = 1
    FAILURE = 2
    ERROR = 3

class TestSummary:
    """Stats keeper"""

    def __init__(self, step_count):
        self.step_count = step_count
        self.succeeded = 0
        self.failed = 0
        self.errors = 0

    def __repr__(self):
        return f'({self.step_count}/{self.succeeded}/{self.failed})'

    def count(self, result):
        """Count step result"""
        if result is Result.SUCCESS:
            self.succeeded += 1
        elif result is Result.FAILURE:
            self.failed += 1
        elif result is Result.ERROR:
            self.errors += 1
        else:
            pass

    def successful(self):
        return self.step_count == self.succeeded

def empty(iterable):
    try:
        first = next(iterable)
    except StopIteration:
        return True
    return False

def check_output(reference, out):
    def diff(ref_lines, out_lines):
        delta = difflib.unified_diff(ref_lines, out_lines)
        if empty(delta):
            return True
        sys.stdout.writelines(delta)
        return False
    if str(reference).endswith('.gz'):
        with gzip.open(reference, mode='rt') as ref:
            return diff(sorted(ref.readlines()), sorted(out.readlines()))
    else:
        with open(reference) as ref:
            return diff(ref.readlines(), sorted(out.readlines()))

def run_step(basecmd, step_id, step, work_dir, baseline_dir, update_baseline):
    def try_wait(process, timeout):
        """Wait for a specified time or terminate the process"""
        try:
            if process.wait(timeout) is not 0:
                LOGGER.error(f'{process.args} returned {process.returncode}')
                return Result.ERROR
            return Result.SUCCESS
        except subprocess.TimeoutExpired:
            LOGGER.error(f'timeout reached, terminating process')
            process.terminate()
            return Result.ERROR
    try:
        out = open(work_dir / f'{step_id}.out', 'w+')
        err = open(work_dir / f'{step_id}.err', 'w')
        cmd = basecmd + step.command
        info_string = ' '.join(map(str, cmd))
        client = spawn(
            basecmd + step.command,
            stdin=subprocess.PIPE,
            stdout=out,
            stderr=err,
            cwd=work_dir)
        start_time = now()
        if step.input:
            incmd = []
            if str(step.input).endswith('gz'):
                incmd += ['gunzip', '-c', str(step.input)]
            else:
                incmd += ['cat', str(step.input)]
            info_string = ' '.join(incmd) + ' | ' + info_string
            input_p = spawn(incmd, stdout=client.stdin)
            result = try_wait(
                input_p, timeout=STEP_TIMEOUT - (now() - start_time))
            client.stdin.close()
        result = try_wait(client, timeout=STEP_TIMEOUT - (now() - start_time))
        if result is Result.ERROR:
            return result
        reference = baseline_dir / f'{step_id}.ref'
        out.seek(0)
        if update_baseline:
            LOGGER.info('updating baseline')
            if not baseline_dir.exists():
                baseline_dir.mkdir(parents=True)
            with open(reference, 'w') as ref:
                for line in sorted(out.readlines()):
                    ref.write(line)
            return Result.SUCCESS
        if not reference.exists():
            LOGGER.error('no baseline found')
            return Result.FAILURE
        LOGGER.info('comparing test output to baseline')
        if check_output(reference, out):
            LOGGER.info('baseline comparison succeeded')
            return Result.SUCCESS
            LOGGER.info('baseline comparison failed')
        # TODO: print diff of failure
        return Result.FAILURE
    except subprocess.CalledProcessError as err:
        LOGGER.error(err)
        return Result.ERROR

class Server:
    """Server fixture implementation details
    """

    def __init__(self,
                 app,
                 args,
                 work_dir,
                 name='node',
                 port=VAST_PORT,
                 **kwargs):
        self.app = app
        self.name = name
        self.cwd = work_dir / self.name
        self.port = port
        LOGGER.info(f'waiting for port {self.port} to be available')
        if not wait.tcp.closed(self.port, timeout=5):
            raise RuntimeError(
                'Port is blocked by another process.\nAborting tests...')
        self.cwd.mkdir(parents=True)
        out = open(self.cwd / 'out', 'w')
        err = open(self.cwd / 'err', 'w')
        self.process = spawn(
            [self.app] + args,
            cwd=self.cwd,
            stdout=out,
            stderr=err,
            **kwargs)
        LOGGER.info(f'waiting for server to listen on port {self.port}')
        if not wait.tcp.open(self.port, timeout=5):
            raise RuntimeError(
                'Server could not aquire port.\nAborting tests')

    def stop(self):
        """Stops the server"""
        LOGGER.info('stopping server')
        stop_out = open(self.cwd / 'stop.out', 'w')
        stop_err = open(self.cwd / 'stop.err', 'w')
        stop = 0
        try:
            stop = spawn([self.app, '-e', f':{self.port}', 'stop'],
                         cwd=self.cwd,
                         stdout=stop_out,
                         stderr=stop_err).wait(STEP_TIMEOUT)
        except:
            stop.kill()
        try:
            self.process.wait(STEP_TIMEOUT)
        except:
            self.process.kill()

class Tester:
    """Test runner
    """

    def __init__(self, args, fixtures):
        self.args = args
        self.app = args.app
        self.cmd = Path(args.app).resolve()
        self.fixtures = fixtures
        self.test_dir = args.directory
        self.update = args.update

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with suppress(OSError):
            self.test_dir.rmdir()

    def check_skip(self, test):
        """Checks if a test should run if a condition is defined"""
        if test.condition:
            check = spawn(self.app + ' ' + test.condition, shell=True)
            ret = check.wait(STEP_TIMEOUT)
            return ret
        return False

    def run(self, test_name, test):
        """Runs a single test"""
        LOGGER.debug(f'running test: {test_name}')
        normalized_test_name = test_name.replace(' ', '-').lower()
        baseline_dir = self.args.set.parent / 'reference' / normalized_test_name
        work_dir = self.test_dir / normalized_test_name
        if work_dir.exists():
            LOGGER.debug(f'removing existing work directory {work_dir}')
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True)
        summary = TestSummary(len(test.steps))
        step_i = 0
        # Locate fixture.
        dummy_fixture = Fixture('pass', 'pass')
        fixture = dummy_fixture if not test.fixture else self.fixtures.get(
            test.fixture)
        cmd = [self.cmd]
        fenter = Template(fixture.enter).substitute(locals())
        fexit = Template(fixture.exit).substitute(locals())
        # Invoke test.
        exec(fenter)
        if self.args.flamegraph:
            svg_file = work_dir / f'{normalized_test_name}.svg'
            run_flamegraph(self.args, svg_file)
        for step in test.steps:
            step_id = 'step_{:02d}'.format(step_i)
            LOGGER.info(f'running step {step_i}: {step.command}')
            result = run_step(cmd, step_id, step, work_dir, baseline_dir,
                              self.update)
            summary.count(result)
            if result is Result.ERROR:
                break
            step_i += 1
        exec(fexit)
        # Summarize result.
        if not self.args.keep and summary.successful():
            LOGGER.debug(f'removing working directory {work_dir}')
            shutil.rmtree(work_dir)
        if summary.successful():
            LOGGER.info(f'ran all {summary.step_count} steps successfully')
        else:
            LOGGER.error(f'ran {summary.succeeded}/{summary.step_count} '
                         'steps successfully')
        return summary.successful()

def validate(data, set_dir):
    def is_file(path):
        return path.is_file()
    def to_fixture(data):
        return Fixture(**data)
    def to_step(data):
        return Step(**data)
    def to_test(data):
        return Test(**data)
    def absolute_path(path):
        absolute = Path(os.path.expanduser(path))
        if not absolute.is_absolute():
            absolute = (set_dir / path).resolve()
        return absolute
    def to_command(raw_command):
        return shlex.split(raw_command.replace('@.', str(set_dir)))
    fixture = schema.Schema(
        schema.And({
            'enter': schema.And(str, len),
            'exit': schema.And(str, len)
        }, schema.Use(to_fixture)))
    fixtures = schema.Schema({schema.And(str, len): fixture})
    step = schema.Schema(
        schema.And({
            'command':
            schema.And(
                schema.Const(schema.And(str, len)), schema.Use(to_command)),
            schema.Optional('input', default=None):
            schema.And(schema.Use(absolute_path), is_file)
        }, schema.Use(to_step)))
    test = schema.Schema(
        schema.And({
            schema.Optional('tags', default=None): [str],
            schema.Optional('condition', default=None): str,
            schema.Optional('fixture', default=None): str,
            'steps': [step]
        }, schema.Use(to_test)))
    tests = schema.Schema({schema.And(str, len): test})
    sch = schema.Schema({'fixtures': fixtures, 'tests': tests})
    return sch.validate(data)

def tagselect(tags, tests):
    if not tags:
        return {k: t for k, t in tests.items() if 'disabled' not in t.tags}
    tagset = set(tags)
    return {k: t for k, t in tests.items() if tagset & set(t.tags)}

def run(args, test_dec):
    tests = test_dec['tests']
    selected_tests = {}
    explicit_tests = {}
    if args.test:
        # Create a subset with only the submitted tests present as keys
        explicit_tests = {k: tests[k] for k in tests.keys() & args.test}
    if not args.test or args.tag:
        selected_tests = tagselect(args.tag, tests)
    tests = dict(selected_tests, **explicit_tests)
    try:
        with Tester(args, test_dec['fixtures']) as tester:
            result = True
            for name, definition in tests.items():
                # Skip the test if the condition is not fulfilled
                if tester.check_skip(definition):
                    LOGGER.debug(f'skipping test {name}')
                    continue
                LOGGER.info(f'executing test: {name}')
                if not tester.run(name, definition):
                    result = False
            return result
    except Exception as err:
        signal_subprocs(signal.SIGTERM)
        raise err

def main():
    """The main function"""
    parser = argparse.ArgumentParser(
        description='Test runner',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--app',
        default='./core',
        help='Path to the executable (vast/core)')
    parser.add_argument(
        '-s',
        '--set',
        type=Path,
        help='Run the testset from this test definition YAML file')
    parser.add_argument(
        '-T', '--tag', nargs='+', help='The tag for which tests will be run')
    parser.add_argument(
        '-t',
        '--test',
        nargs='+',
        help='The test(s) to run (runs all tests if unset)')
    parser.add_argument(
        '-u',
        '--update',
        action='store_true',
        help='Update baseline for tests')
    parser.add_argument(
        '-d',
        '--directory',
        default='run_<current_ISO_timestamp>',
        type=Path,
        help='The basedir for the test runs')
    parser.add_argument(
        '-K',
        '--keep',
        action='store_true',
        help='Keep artifacts of successful runs')
    parser.add_argument(
        '--timeout',
        type=int,
        default=0,
        help='Test timeout in seconds')
    parser.add_argument(
        '-l',
        '--list',
        nargs='*',
        help='Return a list of available tests optionally filtered with tags')
    parser.add_argument(
        '-L',
        '--list-tags',
        action='store_true',
        help='Return a list of all available tags')
    parser.add_argument(
        '--flamegraph',
        action='store_true',
        help='Generate a flamegraph of the test run')
    parser.add_argument(
        '--flamegraph_path',
        default='scripts/flamegraph',
        type=Path,
        help='Path to flamegraph script')
    parser.add_argument(
        '-v',
        '--verbosity',
        default='DEBUG',
        help='Set the logging verbosity')
    args = parser.parse_args()
    # Setup logging.
    LOGGER.setLevel(args.verbosity)
    fmt = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
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
        args.set = Path(__file__).resolve().parent / 'default_set.yaml'
    args.set = args.set.resolve()
    LOGGER.debug(f'resolved test set path to {args.set}')
    test_file = open(args.set, 'r')
    test_dict = yaml.full_load(test_file)
    test_dec = validate(test_dict, args.set.parent)
    # Print tests.
    if args.list is not None:
        selection = tagselect(args.list, test_dec['tests'])
        for test in selection.keys():
            print(test)
        return
    # Print test tags.
    if args.list_tags:
        tags = set().union(
            *[set(t.tags) for _, t in test_dec['tests'].items()])
        for tag in tags:
            print(tag)
        return
    # Create working directory.
    if args.directory.name == 'run_<current_ISO_timestamp>':
        timestamp = datetime.now().isoformat(timespec='seconds')
        args.directory = Path(f'run_{timestamp}')
    LOGGER.debug(f'keeping state in {args.directory}')
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

if __name__ == '__main__':
    main()
