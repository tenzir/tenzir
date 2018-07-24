#!/usr/bin/env python

import argparse
import subprocess
import sys
from collections import namedtuple
from pathlib import Path
from shutil import rmtree

Step = namedtuple('Step', ['command', 'input', 'reference'])
Step_ = namedtuple('Step_', ['command', 'input'])
Test = namedtuple('Test', ['name', 'steps'])


def run_flamegraph(args):
    flamegraph = Path(args.flamegraph_path).resolve()
    test_dir = Path('vast-integration-test')
    with open(test_dir / "bro_conn.svg", 'w') as svg:
        subprocess.Popen(
            [flamegraph, '-s', str(args.timeout)], stdout=svg, cwd=test_dir)


class NodeTester:
    def __init__(self, args):
        self.args = args
        self.vast = Path(args.vast).resolve()

    def run(self, testDef):
        call = [self.vast, '-n']
        work_dir = test_dir / testDef.name
        work_dir.mkdir()
        for step in testDef.steps:
            infile = Path('libvast/test') / step.input
            with open(infile) as input:
                try:
                    if self.args.flamegraph:
                        subprocess.run(['sudo', '-v'])
                        run_flamegraph(self.args)
                    subprocess.run(
                        call + step.command,
                        stdin=input,
                        cwd=work_dir,
                        timeout=self.args.timeout)
                except subprocess.SubprocessError as err:
                    print('Error: ', err)


default_set = [
    Test('bro conn import',
         [Step(['import', 'bro'], Path('logs/bro/conn.log'), '')]),
    Test('mrt import',
         [Step(['import', 'mrt'], Path('logs/mrt/updates20150505.0'), '')])
]


def main():
    global test_dir
    test_dir = Path('vast-integration-test')
    if test_dir.is_dir():
        rmtree(test_dir)
    elif test_dir.exists():
        sys.exit(test_dir + " exists, but is not a directory. Aborting...")

    test_dir.mkdir(parents=True)

    parser = argparse.ArgumentParser(
        description='Test runner',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--vast', default='build/bin/vast', help='Path to vast binary')
    parser.add_argument(
        '--flamegraph',
        action="store_true",
        help='Generate a flamegraph of the test run')
    parser.add_argument(
        '--flamegraph_path',
        default='scripts/flamegraph',
        help='Path to flamegraph script')
    parser.add_argument(
        '--timeout', type=int, default=8, help='Test timeout in seconds')

    args = parser.parse_args()
    nt = NodeTester(args)

    for test in default_set:
        nt.run(test)


if __name__ == "__main__":
    main()
