import os
import subprocess
import logging
import base64
from threading import Thread

logging.getLogger().setLevel(logging.INFO)


class ReturningThread(Thread):
    """A wrapper around the Thread class to actually return the threaded function
    return value when calling join()"""

    def __init__(
        self, group=None, target=None, name=None, args=(), kwargs={}, Verbose=None
    ):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args):
        Thread.join(self, *args)
        return self._return


def buff_and_print(stream, stream_name):
    """Buffer and log every line of the given stream"""
    buff = []
    for l in iter(lambda: stream.readline(), b""):
        line = l.decode("utf-8")
        logging.info("%s: %s", stream_name, line.rstrip())
        buff.append(line)
    return "".join(buff)


def handler(event, context):
    """An AWS Lambda handler that runs the provided command with bash and returns the standard output"""
    try:
        # input parameters
        logging.debug("event: %s", event)
        src_cmd = base64.b64decode(event["cmd"]).decode("utf-8")
        host = event["host"]
        logging.info("src_cmd: %s", src_cmd)
        logging.info("host: %s", host)

        # execute the command as bash and return the std outputs
        parsed_cmd = ["/bin/bash", "-c", src_cmd]
        os.environ["VAST_ENDPOINT"] = host
        process = subprocess.Popen(
            parsed_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        # we need to spin up a thread to avoid deadlock when reading through output pipes
        stderr_thread = ReturningThread(
            target=buff_and_print, args=(process.stderr, "stderr")
        )
        stderr_thread.start()
        stdout = buff_and_print(process.stdout, "stdout")
        stderr = stderr_thread.join()

        # multiplex stdout and stderr into the result field
        res = stdout if stdout != "" else stderr
        return {"result": res, "parsed_cmd": parsed_cmd}

    except Exception as err:
        err_string = "Exception caught: {0}".format(err)
        logging.error(err_string)
        return {"result": err_string}
