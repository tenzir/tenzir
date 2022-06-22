import os
import subprocess
import logging
import base64
import sys
import io
from threading import Thread

logging.getLogger().setLevel(logging.INFO)
__stdout__ = sys.stdout


class ReturningThread(Thread):
    """A wrapper around the Thread class to actually return the threaded function
    return value when calling join()"""

    def __init__(self, target=None, args=()):
        Thread.__init__(self, target=target, args=args)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args):
        Thread.join(self, *args)
        return self._return


class CommandException(Exception):
    ...


def hide_command_exception(func):
    """Block printing on CommandException to avoid logging stderr twice"""

    def func_wrapper(*args, **kwargs):
        # reset stdout to its original value that we saved on init
        sys.stdout = __stdout__
        try:
            return func(*args, **kwargs)
        except CommandException as e:
            # the error will be printed to a disposable buffer
            sys.stdout = io.StringIO()
            raise e

    return func_wrapper


def buff_and_print(stream, stream_name):
    """Buffer and log every line of the given stream"""
    buff = []
    for l in iter(lambda: stream.readline(), b""):
        line = l.decode("utf-8")
        logging.info("%s: %s", stream_name, line.rstrip())
        buff.append(line)
    return "".join(buff)


@hide_command_exception
def handler(event, context):
    """An AWS Lambda handler that runs the provided command with bash and returns the standard output"""
    # input parameters
    logging.debug("event: %s", event)
    src_cmd = base64.b64decode(event["cmd"]).decode("utf-8")
    logging.info("src_cmd: %s", src_cmd)
    if "env" in event:
        logging.info("env: %s", event["env"])
        for (k, v) in event["env"].items():
            os.environ[k] = v

    # execute the command as bash and return the std outputs
    parsed_cmd = ["/bin/bash", "-c", src_cmd]
    process = subprocess.Popen(
        parsed_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    # we need to spin up a thread to avoid deadlock when reading through output pipes
    stderr_thread = ReturningThread(
        target=buff_and_print, args=(process.stderr, "stderr")
    )
    stderr_thread.start()
    stdout = buff_and_print(process.stdout, "stdout").strip()
    stderr = stderr_thread.join().strip()
    returncode = process.wait()
    logging.info("returncode: %s", returncode)

    if returncode != 0:
        raise CommandException(stderr)
    return {
        "stdout": stdout,
        "stderr": stderr,
        "parsed_cmd": parsed_cmd,
    }
