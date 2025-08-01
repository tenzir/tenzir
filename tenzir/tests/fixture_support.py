import argparse
import subprocess
from pathlib import Path

class Executor :
    def __init__( self ) :
        parser = argparse.ArgumentParser()
        parser.add_argument("--endpoint", type=str)
        parser.add_argument("--binary", type=str)
        args = parser.parse_args()
        self.binary = args.binary
        self.endpoint = args.endpoint

    def run(self, source:str) -> subprocess.CompletedProcess[bytes]:
        cmd = [
            self.binary,
            "--bare-mode",
            f"--endpoint={self.endpoint}",
            "--console-verbosity=warning",
            "--multi",
            source,
        ]
        return subprocess.run(
            cmd,
            timeout=10,
            stdout=subprocess.PIPE
        )
