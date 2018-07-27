#!/usr/bin/env python

import argparse
import subprocess
import sys
from collections import namedtuple
from pathlib import Path
from shutil import rmtree

Step = namedtuple('Step', ['command', 'input', 'reference'])
Test = namedtuple('Test', ['name', 'steps'])

TEST_DIR = Path('vast-integration-test')


def run_flamegraph(args, svg_file):
    """Perform instrumentation and produce an output svg for """
    flamegraph = Path(args.flamegraph_path).resolve()
    with open(svg_file, 'w') as svg:
        # refresh sudo credential cache in a blocking call to be sure to catch
        # the entire test run
        subprocess.run(['sudo', '-v'])
        return subprocess.Popen(
            [flamegraph, '-s', str(args.timeout)], stdout=svg, cwd=TEST_DIR)


class TestSummary:
    def __init__(self, test_def):
        self.test_def = test_def
        self.succeeded = 0
        self.failed = 0

    def print(self):
        print('({}/{}/{})'.format(len(self.test_def.steps),
                                  self.succeeded, self.failed))


class ServerTester:
    def __init__(self, args):
        self.args = args
        self.vast = Path(args.vast).resolve()

    def run(self, test_def):
        """Runs a single test"""
        call = [self.vast]
        work_dir = TEST_DIR / test_def.name
        work_dir.mkdir()
        test_summary = TestSummary(test_def)
        if self.args.flamegraph:
            svg_file = work_dir / '{}.svg'.format(test_def.name)
            run_flamegraph(self.args, svg_file)
        server = subprocess.Popen(
            call + ['-d', test_def.name, 'start'],
            cwd=TEST_DIR)
        for step in test_def.steps:
            infile = Path('libvast/test') / step.input
            with open(infile) as input_data:
                try:
                    subprocess.check_call(
                        call + step.command,
                        stdin=input_data,
                        cwd=work_dir,
                        timeout=self.args.timeout)
                except subprocess.CalledProcessError as err:
                    test_summary.failed += 1
                    print('Error: ', err)
                else:
                    test_summary.succeeded += 1
        if server.wait(timeout=self.args.timeout) == 0:
            # Clean up after successful run
            rmtree(work_dir)
        test_summary.print()


class NodeTester:
    def __init__(self, args):
        self.args = args
        self.vast = Path(args.vast).resolve()

    def run(self, test_def):
        """Runs a single test"""
        call = [self.vast, '-n']
        work_dir = TEST_DIR / test_def.name
        work_dir.mkdir()
        test_summary = TestSummary(test_def)
        for i, step in enumerate(test_def.steps):
            infile = Path('libvast/test') / step.input
            with open(infile) as input_data:
                try:
                    if self.args.flamegraph:
                        svg_file = work_dir / \
                            '{}_{}.svg'.format(test_def.name, str(i))
                        run_flamegraph(self.args, svg_file)
                    subprocess.check_call(
                        call + step.command,
                        stdin=input_data,
                        cwd=work_dir,
                        timeout=self.args.timeout)
                except subprocess.CalledProcessError as err:
                    test_summary.failed += 1
                    print('Error: ', err)
                else:
                    test_summary.succeeded += 1
        if test_summary.failed == 0:
            # Clean up after successful run
            rmtree(work_dir)
        test_summary.print()


DEFAULT_SET = [
    Test('bro conn import',
         [Step(['import', 'bro'], Path('logs/bro/conn.log'), '')]),
    Test('mrt import',
         [Step(['import', 'mrt'], Path('logs/mrt/updates20150505.0'), '')])
]


def main():
    """The main function"""
    if TEST_DIR.is_dir():
        rmtree(TEST_DIR)
    elif TEST_DIR.exists():
        sys.exit(TEST_DIR + ' exists, but is not a directory. Aborting...')

    TEST_DIR.mkdir(parents=True)

    parser = argparse.ArgumentParser(
        description='Test runner',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--vast', default='build/bin/vast', help='Path to vast binary')
    parser.add_argument(
        '--flamegraph',
        action='store_true',
        help='Generate a flamegraph of the test run')
    parser.add_argument(
        '--flamegraph_path',
        default='scripts/flamegraph',
        help='Path to flamegraph script')
    parser.add_argument(
        '--timeout', type=int, default=8, help='Test timeout in seconds')

    args = parser.parse_args()

    #nt = NodeTester(args)
    # for test in DEFAULT_SET:
    #    nt.run(test)

    st = ServerTester(args)
    for test in DEFAULT_SET:
        st.run(test)


if __name__ == '__main__':
    main()
