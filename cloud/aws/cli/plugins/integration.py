import filecmp
import flags
import unittest
from vast_invoke import task, Context
import time
import json
import os
from common import AWS_REGION, COMMON_VALIDATORS, AWS_REGION_VALIDATOR, container_path

VALIDATORS = [
    *COMMON_VALIDATORS,
    AWS_REGION_VALIDATOR,
]

# A common prefix for all test files to help cleanup
TEST_PREFIX = "vastcloudtest"

# Force stack trace display for all tests
os.environ[flags.TRACE_FLAG_VAR] = "1"


def vast_import_suricata(c: Context):
    """Import Suricata data from the provided URL"""
    url = "https://raw.githubusercontent.com/tenzir/vast/v1.0.0/vast/integration/data/suricata/eve.json"
    c.run(
        f"./vast-cloud execute-command -c 'wget -O - -o /dev/null {url} | vast import suricata'"
    )


@task
def clean(c):
    """Delete local and remote files starting with the TEST_PREFIX"""
    print("Cleaning up...")
    tmp_prefix = f"/tmp/{TEST_PREFIX}*"
    c.run(f"rm -rfv {container_path(tmp_prefix)}")
    bucket_name = c.run("./vast-cloud workbucket.name", hide="out").stdout.rstrip()
    c.run(
        f'aws s3 rm --recursive s3://{bucket_name}/ --exclude "*" --include "{TEST_PREFIX}*" --region {AWS_REGION()}'
    )


class SingleTestWithContext(unittest.TestCase):
    """A TestCase with an Invoke Context and a single test method called "test" """

    def __init__(self, invoke_ctx: Context) -> None:
        self.c = invoke_ctx
        super().__init__("test")


class VastStartRestart(SingleTestWithContext):
    def test(self):
        """Validate VAST server start and restart commands"""
        print("Run start-vast-server")
        self.c.run("./vast-cloud start-vast-server")

        print("Get running vast server")
        self.c.run("./vast-cloud get-vast-server")

        print("Run start-vast-server again")
        self.c.run("./vast-cloud start-vast-server")

        print("Run restart-vast-server")
        self.c.run("./vast-cloud restart-vast-server")

        print("Get running vast server")
        self.c.run("./vast-cloud get-vast-server")

        print("The task needs a bit of time to boot, sleeping for a while...")
        time.sleep(100)


class LambdaOutput(SingleTestWithContext):
    def test(self):
        """Validate VAST server start and restart commands"""
        print("Running successful command")
        success = self.c.run(
            "./vast-cloud run-lambda -c 'echo hello' -e VAR1=val1", hide="out"
        ).stdout
        self.assertEqual(
            success,
            """PARSED COMMAND:
['/bin/bash', '-c', 'echo hello']

ENV:
{'VAR1': 'val1'}

STDOUT:
hello

STDERR:


RETURN CODE:
0
""",
        )

        print("Running erroring command")
        error = self.c.run(
            "./vast-cloud run-lambda -c 'echo hello >&2 && false' -e VAR1=val1",
            hide="out",
            warn=True,
        ).stdout
        self.assertEqual(
            error,
            """PARSED COMMAND:
['/bin/bash', '-c', 'echo hello >&2 && false']

ENV:
{'VAR1': 'val1'}

STDOUT:


STDERR:
hello

RETURN CODE:
1
""",
        )


class VastDataImport(SingleTestWithContext):
    def vast_count(self):
        """Run `vast count` and parse the result"""
        res_raw = self.c.run(
            './vast-cloud run-lambda -c "vast count" --json-output', hide="stdout"
        )
        res_obj = json.loads(res_raw.stdout)
        self.assertEqual(
            res_obj["parsed_cmd"],
            [
                "/bin/bash",
                "-c",
                "vast count",
            ],
            "Unexpected parsed command",
        )
        self.assertTrue(res_obj["stdout"].isdigit(), "Count result is not a number")
        return int(res_obj["stdout"])

    def test(self):
        """Import data from a file and check count increase"""
        print("Import data into VAST")
        init_count = self.vast_count()
        vast_import_suricata(self.c)
        new_count = self.vast_count()
        print(f"Expected count increase 7, got {new_count-init_count}")
        self.assertEqual(7, new_count - init_count, "Wrong count")


class WorkbucketRoundtrip(SingleTestWithContext):
    def validate_dl(self, src, dst, key):
        print(f"Download object {key} to {dst}")
        self.c.run(
            f"./vast-cloud workbucket.download --destination={dst} --key={key}",
            hide="out",
        )
        self.assertTrue(
            filecmp.cmp(container_path(src), container_path(dst)),
            f"{src} and {dst} are not identical",
        )

    def setUp(self):
        clean(self.c)

    def tearDown(self):
        clean(self.c)

    def test(self):
        """Validate that the roundtrip upload/download works"""
        key_fromfile = f"{TEST_PREFIX}_fromfile"
        key_frompipe = f"{TEST_PREFIX}_frompipe"
        src = f"/tmp/{TEST_PREFIX}_src"
        dst_fromfile = f"/tmp/{TEST_PREFIX}_fromfile_dst"
        dst_frompipe = f"/tmp/{TEST_PREFIX}_frompipe_dst"
        self.c.run(f"echo 'hello world' > {container_path(src)}")

        print("List before upload")
        empty_ls = self.c.run(
            f"./vast-cloud workbucket.list --prefix={TEST_PREFIX}", hide="out"
        ).stdout
        self.assertEqual(empty_ls, "")

        print(f"Upload from {src} to object {key_fromfile}")
        self.c.run(
            f"./vast-cloud workbucket.upload --source={src} --key={key_fromfile}"
        )

        print(f"Upload from stdin to object {key_frompipe}")
        self.c.run(
            f"echo 'hello world' | ./vast-cloud workbucket.upload --source=/dev/stdin --key={key_frompipe}"
        )

        print(f"List after upload")
        ls = self.c.run(
            f"./vast-cloud workbucket.list --prefix {TEST_PREFIX}", hide="out"
        ).stdout
        self.assertIn(key_fromfile, ls)
        self.assertIn(key_frompipe, ls)

        self.validate_dl(src, dst_fromfile, key_fromfile)

        self.validate_dl(src, dst_frompipe, key_frompipe)

        print(f"Delete object {key_frompipe}")
        self.c.run(f"./vast-cloud workbucket.delete --key={key_frompipe}", hide="out")
        half_ls = self.c.run(
            f"./vast-cloud workbucket.list --prefix {TEST_PREFIX}", hide="out"
        ).stdout
        self.assertIn(key_fromfile, half_ls)
        self.assertNotIn(key_frompipe, half_ls)


class ScriptedCmd(SingleTestWithContext):
    def setUp(self):
        clean(self.c)

    def tearDown(self):
        clean(self.c)

    def test(self):
        """Validate that we can run commands from files"""
        script = """
    VAR1="hello"
    echo -n $VAR1
    echo -n " "
    echo -n $VAR2
    """
        script_file = f"/tmp/{TEST_PREFIX}_script"
        script_key = f"{TEST_PREFIX}_script"
        with open(container_path(script_file), "w") as text_file:
            text_file.write(script)

        print("Run lambda with local script")
        res = self.c.run(
            f"./vast-cloud run-lambda -c file://{script_file} -e VAR2=world --json-output",
            hide="out",
        ).stdout
        self.assertEqual(json.loads(res)["stdout"], "hello world")

        print("Run lambda with piped script")
        res = self.c.run(
            f"cat {container_path(script_file)} | ./vast-cloud run-lambda -c file:///dev/stdin -e VAR2=world --json-output",
            hide="out",
        ).stdout
        self.assertEqual(json.loads(res)["stdout"], "hello world")

        print("Run execute command with piped script")
        # we can pipe into execute command here because we are in a no pty context
        res = self.c.run(
            f"cat {container_path(script_file)} | ./vast-cloud execute-command -c file:///dev/stdin -e VAR2=world",
            hide="out",
        ).stdout
        self.assertIn("hello world", res)

        print("Run execute command with S3 script")
        self.c.run(
            f"./vast-cloud workbucket.upload --source={script_file} --key={script_key}"
        )
        bucket_name = self.c.run(
            "./vast-cloud workbucket.name", hide="out"
        ).stdout.rstrip()
        res = self.c.run(
            f"./vast-cloud execute-command -c s3://{bucket_name}/{script_key} -e VAR2=world",
            hide="out",
        ).stdout
        self.assertIn("hello world", res)


@task
def lambda_output(c):
    unittest.TextTestRunner().run(LambdaOutput(c))


@task
def vast_start_restart(c):
    unittest.TextTestRunner().run(VastStartRestart(c))


@task
def vast_data_import(c):
    unittest.TextTestRunner().run(VastDataImport(c))


@task
def workbucket_roundtrip(c):
    unittest.TextTestRunner().run(WorkbucketRoundtrip(c))


@task
def scripted_cmd(c):
    unittest.TextTestRunner().run(ScriptedCmd(c))


@task
def all(c):
    """Run the entire testbook.

    Notes:
    - VAST needs to be deployed beforehand with the workbucket plugin
    - This will affect the state of the current stack"""
    suite = unittest.TestSuite()
    suite.addTest(VastStartRestart(c))
    suite.addTest(LambdaOutput(c))
    suite.addTest(VastDataImport(c))
    suite.addTest(WorkbucketRoundtrip(c))
    suite.addTest(ScriptedCmd(c))
    unittest.TextTestRunner().run(suite)
