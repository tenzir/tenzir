#!/usr/bin/env python3

import argparse
import time
import os
import sys
import subprocess

TENZIR = "../build/release/bin/tenzir"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args()
    tql = None
    watch = []
    for path in args.paths:
        if path.endswith(".tql"):
            if tql is not None:
                sys.exit("exactly one argument must be a `.tql` file")
            tql = path
        else:
            watch.append(path)
    if tql is None:
        sys.exit("exactly one argument must be a `.tql` file")

    def stat():
        result = [os.stat(tql).st_mtime]
        for w in watch:
            result.append(os.stat(w).st_mtime)
        return result

    old = stat()

    def check():
        nonlocal old
        new = stat()
        if old != new:
            old = new
            return True
        return False

    def run():
        p = subprocess.Popen(
            [
                TENZIR,
                "--tql2",
                "--color=always",
                "-q",
                "--implicit-events-sink=to stdout write json -C",
                "-f",
                tql,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        first = True
        for c in iter(lambda: p.stdout.read(1) or p.stderr.read(1), b""):
            if first:
                os.system("clear")
                first = False
            sys.stdout.buffer.write(c)
            sys.stdout.flush()
            if check():
                os.system("clear")
                return
        if first:
            os.system("clear")
        while True:
            if check():
                return
            time.sleep(0.1)

    os.system("clear")
    while True:
        run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
