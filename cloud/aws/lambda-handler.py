import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO)


def handler(event, context):
    """An AWS Lambda handler that runs the provided command with bash and returns the standard output"""
    try:
        logging.info("event: ", event)
        src_cmd = event["cmd"]
        host = event["host"]

        # execute the command as bash and return the std outputs
        parsed_cmd = ["/bin/bash", "-c", src_cmd]
        os.environ["VAST_ENDPOINT"] = host
        process = subprocess.Popen(
            parsed_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        res = stdout if stdout != b"" else stderr

        return {"result": res.decode("utf-8"), "parsed_cmd": parsed_cmd}

    except Exception as err:
        err_string = "Exception caught: {0}".format(err)
        logging.error(err_string)
        return {"result": err_string}
