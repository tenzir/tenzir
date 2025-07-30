import subprocess
from pathlib import Path
import os
import sys

class Executor :
    def __init__( self ) :
        self.binary = os.environ["TENZIR_PYTHON_FIXTURE_BINARY"]
        self.endpoint = os.environ.get("TENZIR_PYTHON_FIXTURE_ENDPOINT")

    def run(self, source:str, timeout:int = 10, mirror:bool=False) -> subprocess.CompletedProcess[bytes]:
        cmd = [
            self.binary,
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
        ]
        if self.endpoint != None :
            cmd.append(f"--endpoint={self.endpoint}")
        cmd.append(source)
        res = subprocess.run(
            cmd,
            timeout=timeout,
            capture_output=True
        )
        if mirror :
            if res.stdout and len(res.stdout) != 0 :
                print(res.stdout.decode())
            if res.stderr and len(res.stderr) != 0 :
                print(res.stdout.decode(),file=sys.stderr)
        return res
