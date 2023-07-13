"""[Tenzir Pro] Download the Tenzir pro image"""

from tenzir_invoke import pty_task, task
import dynaconf
from common import conf

VALIDATORS = [dynaconf.Validator("TENZIR_VERSION", must_exist=True, ne="")]


@pty_task
def login(c):
    """Login to the registry where the Tenzir Pro images are stored"""
    c.run("gcloud auth login --no-launch-browser")
    c.run("gcloud auth configure-docker --quiet")


@task
def pull_image(c):
    """Pull a Tenzir Pro image. You need to login first."""
    source_tag = "eu.gcr.io/crucial-kayak-261816/tenzir-pinned"
    output_tag = "tenzir/tenzir-pro"
    version = conf(VALIDATORS)["TENZIR_VERSION"]
    c.run(f"docker pull {source_tag}:{version}")
    c.run(f"docker image tag {source_tag}:{version} {output_tag}:{version}")
    c.run(f"docker rmi {source_tag}:{version}")
    print("============")
    print("Tenzir Pro image successfully pulled")
    print(f"Set the variable TENZIR_IMAGE={output_tag}")
    print("Then run `./tenzir-cloud deploy` to deploy Tenzir with the pulled Pro version")
