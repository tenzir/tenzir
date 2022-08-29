"""This cloud plugin offers helpers to download the VAST Pro image from the
Tenzir private repository"""

from vast_invoke import pty_task, task
import dynaconf
from common import conf

VALIDATORS = [dynaconf.Validator("VAST_VERSION", must_exist=True, ne="")]


@pty_task
def login(c):
    """Login to the registry where the VAST Pro images are stored"""
    c.run("gcloud auth login --no-launch-browser")
    c.run("gcloud auth configure-docker --quiet")


@task
def pull_image(c):
    """Pull a VAST Pro image. You need to login first."""
    source_tag = "eu.gcr.io/crucial-kayak-261816/vast-pinned"
    output_tag = "tenzir/vast-pro"
    version = conf(VALIDATORS)["VAST_VERSION"]
    c.run(f"docker pull {source_tag}:{version}")
    c.run(f"docker image tag {source_tag}:{version} {output_tag}:{version}")
    c.run(f"docker rmi {source_tag}:{version}")
    print("============")
    print("VAST Pro image successfully pulled")
    print(f"Set the variable VAST_IMAGE={output_tag}")
    print("Then run `./vast-cloud deploy` to deploy VAST with the pulled Pro version")
