#!/usr/bin/env python
"""tenzir core / vast integration test script v1

"""

import argparse
from contextlib import suppress
from datetime import datetime
import difflib
from enum import Enum
import filecmp
import gzip
import itertools
import os
from pathlib import Path
import shlex
import shutil
import signal
from string import Template
import subprocess
import sys
import time
from typing import Callable, List, NamedTuple, Optional, TypeVar

import schema
import yaml

import packages.wait as wait

VAST_PORT = 42024
STEP_TIMEOUT = 30
CURRENT_SUBPROCS: List[subprocess.Popen] = []

PARENT = Path(__file__).resolve().parent


class Fixture(NamedTuple):
    enter: str
    exit: str


class Step(NamedTuple):
    command: List[str]
    input: Optional[Path]
    reference: Optional[Path]


class Test(NamedTuple):
    tags: Optional[str]
    condition: Optional[str]
    fixture: Optional[str]
    steps: List[Step]


def signal_subprocs(signum):
    """send signal recieved to subprocesses"""
    for proc in reversed(CURRENT_SUBPROCS):
        if proc.poll() is None:
            print('Sending signal {} to {}'.format(signum, proc.args))
            proc.send_signal(signum)


def handle_exit_signal(signum, _frame):
    """send signal recieved to subprocesses and exit"""
    print('Got signal {}, shutting down'.format(signum))
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
        return '({}/{}/{})'.format(self.step_count, self.succeeded,
                                   self.failed)

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
    def impl(ref_lines, out_lines):
        diff = difflib.unified_diff(ref_lines, out_lines)
        if empty(diff):
            return True
        sys.stdout.writelines(diff)
        return False

    if str(reference).endswith('.gz'):
        with gzip.open(reference, mode='rt') as ref:
            return impl(sorted(ref.readlines()), sorted(out.readlines()))
    else:
        with open(reference) as ref:
            return impl(ref.readlines(), sorted(out.readlines()))


def run_step(basecmd, step_number, step, work_dir):
    def try_wait(process, timeout):
        """Wait for a specified time or terminate the process"""
        try:
            if process.wait(timeout) is not 0:
                print('Error: {} returned with value {}'.format(
                    process.args, process.returncode))
                return Result.ERROR
            return Result.SUCCESS
        except subprocess.TimeoutExpired:
            print("Timeout reached")
            process.terminate()
            return Result.ERROR

    result = Result.FAILURE
    try:
        step_id = 'step_{:02d}'.format(step_number)
        out = open(work_dir / '{}.out'.format(step_id), 'w+')
        err = open(work_dir / '{}.err'.format(step_id), 'w')
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
            input_p = spawn(incmd, stdout=client.stdin)
            result = try_wait(
                input_p, timeout=STEP_TIMEOUT - (now() - start_time))
            client.stdin.close()
        result = try_wait(client, timeout=STEP_TIMEOUT - (now() - start_time))

        if result is not Result.ERROR and step.reference:
            print('comparing to ref')
            out.seek(0)
            if check_output(step.reference, out):
                print('OK')
                result = Result.SUCCESS
            else:
                print('FAILURE')
                result = Result.FAILURE
    except subprocess.CalledProcessError as err:
        result = Result.ERROR
        print('Error: ', err, file=sys.stderr)
    return result


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
        print("Waiting for port {} to be available".format(self.port))
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
        print("Waiting for server to listen on port {}".format(self.port))
        if not wait.tcp.open(self.port, timeout=5):
            raise RuntimeError(
                'Server could not aquire port.\nAborting tests')

    def stop(self):
        """Stops the server"""
        print('Stopping server')
        stop_out = open(self.cwd / 'stop.out', 'w')
        stop_err = open(self.cwd / 'stop.err', 'w')
        stop = 0
        try:
            stop = spawn([self.app, '-e', ':{}'.format(self.port), 'stop'],
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with suppress(OSError):
            self.test_dir.rmdir()

    def check_skip(self, test):
        """Checks if a test should run if a condition is defined"""
        if test.condition:
            print('checking if test should be skipped: ', end='')
            check = spawn(self.app + ' ' + test.condition, shell=True)
            ret = check.wait(STEP_TIMEOUT)
            print(("No", "Yes")[ret])
            return ret
        return False

    def run(self, test_name, test):
        """Runs a single test"""
        work_dir = self.test_dir / test_name
        work_dir.mkdir(parents=True)
        test_summary = TestSummary(len(test.steps))
        step_i = 0

        dummy_fixture = Fixture('pass', 'pass')
        fixture = dummy_fixture if not test.fixture else self.fixtures.get(
            test.fixture)

        cmd = [self.cmd]
        fenter = Template(fixture.enter).substitute(locals())
        fexit = Template(fixture.exit).substitute(locals())

        exec(fenter)
        if self.args.flamegraph:
            svg_file = work_dir / '{}.svg'.format(test_name)
            run_flamegraph(self.args, svg_file)

        for step in test.steps:
            print('Running step {}'.format(step.command))
            result = run_step(cmd, step_i, step, work_dir)
            test_summary.count(result)
            if result is Result.ERROR:
                break
            step_i += 1
        exec(fexit)

        if not self.args.keep and test_summary.successful():
            # Clean up after successful run
            shutil.rmtree(work_dir)
        print(test_summary)
        return test_summary.successful()


def validate(data):
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
            absolute = (PARENT / path).resolve()
        return absolute

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
                schema.Const(schema.And(str, len)), schema.Use(shlex.split)),
            schema.Optional('input', default=None):
            schema.And(schema.Use(absolute_path), is_file),
            schema.Optional('reference', default=None):
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
                print('')
                print('Test: {}'.format(name))
                # Skip the test if the condition is not fulfilled
                if tester.check_skip(definition):
                    continue
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
    # TODO: add verbose mode to print pasteable commands
    parser.add_argument(
        '--app', default='./core', help='Path to the executable (vast/core)')
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
        '--timeout', type=int, default=0, help='Test timeout in seconds')
    parser.add_argument(
        '--list',
        nargs='*',
        help='Return a list of available tests optionally filtered with tags')
    parser.add_argument(
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

    args = parser.parse_args()
    if not args.set:
        args.set = PARENT / 'default_set.yaml'

    test_file = open(args.set, 'r')
    test_dict = yaml.load(test_file)
    test_dec = validate(test_dict)

    if args.list is not None:
        selection = tagselect(args.list, test_dec['tests'])
        for test in selection.keys():
            print(test)
        return

    if args.list_tags:
        tags = set().union(
            *[set(t.tags) for _, t in test_dec['tests'].items()])
        for tag in tags:
            print(tag)
        return

    if args.directory.name == 'run_<current_ISO_timestamp>':
        args.directory = Path('run_{}'.format(
            datetime.now().isoformat(timespec='seconds')))
    if args.directory.is_dir():
        shutil.rmtree(args.directory)
    elif args.directory.exists():
        sys.exit(args.directory +
                 ' exists, but is not a directory. Aborting...')

    args.directory.mkdir(parents=True)

    signal.alarm(args.timeout)

    success = run(args, test_dec)

    signal.alarm(0)

    with suppress(OSError):
        args.directory.rmdir()

    retcode = 0 if success else 1
    sys.exit(retcode)


if __name__ == '__main__':
    main()
