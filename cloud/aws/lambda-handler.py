import shlex
import subprocess
import logging

logging.basicConfig(level=logging.INFO)


def handler(event, context):
    try:
        logging.info("event: ", event)
        # parse the input command and check that it contains one vast command
        cmd_split = shlex.split(event['cmd'])
        vast_positions = [i for i, x in enumerate(cmd_split) if x == "vast"]
        if len(vast_positions) != 1:
            raise Exception(
                "Command should contain one and only one call to vast")
        # if a host is specified in the event, add it to the command
        if "host" in event:
            cmd_split.insert(vast_positions[0]+1, "-e")
            cmd_split.insert(vast_positions[0]+2, event['host'])
        logging.info("cmd: ", cmd_split)
        # execute the command and return the std outputs
        process = subprocess.Popen(cmd_split,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        res = stdout if stdout != b'' else stderr
        return {"result": res.decode("utf-8"), "parsed_cmd": cmd_split}
    except Exception as err:
        err_string = "Exception caught: {0}".format(err)
        logging.error(err_string)
        return {"result": err_string}
