import shlex
import subprocess


def handler(event, context):
    try:
        print(event)
        process = subprocess.Popen(shlex.split(event['cmd']),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        res = stdout if stdout != b'' else stderr
        return {"result": res.decode("utf-8")}
    except Exception as err:
        err_string = "Exception caught: {0}".format(err)
        print(err_string)
        return {"result": err_string}
