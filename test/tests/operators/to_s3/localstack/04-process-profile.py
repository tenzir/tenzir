# runner: python

import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        calls = root / "calls"
        helper = root / "credentials.py"
        helper.write_text(
            "\n".join(
                [
                    "import json",
                    "from pathlib import Path",
                    f"with Path({str(calls)!r}).open('a') as handle:",
                    "    handle.write('invoked\\n')",
                    "print(json.dumps({",
                    '    "Version": 1,',
                    '    "AccessKeyId": "test",',
                    '    "SecretAccessKey": "test",',
                    '    "SessionToken": "test",',
                    '    "Expiration": "2035-01-01T00:00:00Z",',
                    "}))",
                ]
            )
        )
        config = root / "config"
        command = " ".join([shlex.quote(sys.executable), shlex.quote(str(helper))])
        config.write_text(
            "\n".join(
                [
                    "[profile process-profile]",
                    f"credential_process = {command}",
                    "region = us-east-1",
                ]
            )
        )
        env = os.environ.copy()
        for variable in (
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_ARN",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        ):
            env.pop(variable, None)
        env["AWS_CONFIG_FILE"] = str(config)
        env["AWS_EC2_METADATA_DISABLED"] = "true"
        bucket = env["LOCALSTACK_S3_BUCKET"]
        tenzir = shlex.split(env["TENZIR_BINARY"])

        default_env = env.copy()
        default_env["AWS_PROFILE"] = "process-profile"
        default_env.pop("AWS_DEFAULT_PROFILE", None)
        default_pipeline = "\n".join(
            [
                'from {message: "default process profile"}',
                f'to_s3 "s3://{bucket}/output-default-process-profile.json" '
                "{ write_json }",
            ]
        )
        subprocess.run([*tenzir, default_pipeline], env=default_env, check=True)
        default_invocations = len(calls.read_text().splitlines())
        print(
            "default profile credential process invoked: "
            f"{str(default_invocations > 0).lower()}"
        )

        explicit_env = env.copy()
        explicit_env.pop("AWS_PROFILE", None)
        explicit_env.pop("AWS_DEFAULT_PROFILE", None)
        explicit_pipeline = "\n".join(
            [
                'from {message: "explicit process profile"}',
                f'to_s3 "s3://{bucket}/output-explicit-process-profile.json", '
                'aws_iam={profile: "process-profile"} { write_json }',
            ]
        )
        subprocess.run([*tenzir, explicit_pipeline], env=explicit_env, check=True)
        explicit_invocations = len(calls.read_text().splitlines())
        print(
            "explicit profile credential process invoked: "
            f"{str(explicit_invocations > default_invocations).lower()}"
        )


if __name__ == "__main__":
    main()
