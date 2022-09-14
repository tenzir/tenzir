"""Expose HTTP service with Cloudflare Access"""
import json
import os
import requests
import dynaconf
from vast_invoke import Context, task
from typing import Tuple
from common import (
    FargateService,
    terraform_output,
)

VALIDATORS = [
    dynaconf.Validator("VAST_CLOUDFLARE_ACCOUNT_ID", must_exist=True, ne=""),
    dynaconf.Validator("VAST_CLOUDFLARE_ZONE", must_exist=True, ne=""),
    dynaconf.Validator("VAST_CLOUDFLARE_API_TOKEN", must_exist=True, ne=""),
]


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "cloudflare", "cloudflare_task_family")
    service_name = terraform_output(c, "cloudflare", "cloudflare_service_name")
    return (cluster, service_name, family)


@task
def status(c):
    """Get the status of the cloudflare service"""
    print(FargateService(*service_outputs(c)).service_status())


@task
def start(c):
    """Start the cloudflare as an AWS Fargate task. Noop if it is already running"""
    FargateService(*service_outputs(c)).start_service()


@task
def stop(c):
    """Stop the cloudflare instance and service"""
    FargateService(*service_outputs(c)).stop_service()


@task
def restart(c):
    """Stop the running MISP task, the service starts a new one"""
    FargateService(*service_outputs(c)).restart_service()


class CloudflareClient:
    def __init__(self):
        self.token = os.environ["VAST_CLOUDFLARE_API_TOKEN"]
        self.account_id = os.environ["VAST_CLOUDFLARE_ACCOUNT_ID"]
        self.base_url = "https://api.cloudflare.com/client/v4"

    def _headers(self):
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

    def update_tunnel_config(self, tunnel_id, config):
        """Update the tunnel with the provided tunnel config (following the syntax rules of config.yaml)"""
        resp = requests.put(
            f"{self.base_url}/accounts/{self.account_id}/cfd_tunnel/{tunnel_id}/configurations",
            json=config,
            headers=self._headers(),
        )
        resp.raise_for_status()


@task
def setup(c):
    tunnel_id = terraform_output(c, "cloudflare", "cloudflare_tunnel_id")
    hostnames = terraform_output(c, "cloudflare", "cloudflare_hostnames").split(",")
    # TODO dynamically load exposed services from all module
    misp_url = terraform_output(c, "misp", "exposed_services")
    misp_rule = {
        "hostname": hostnames[0],
        "service": misp_url,
    }
    default_rule = {"service": "http_status:404"}
    CloudflareClient().update_tunnel_config(
        tunnel_id,
        {"config": {"ingress": [misp_rule, default_rule]}},
    )
    print(
        f"""Exposing apps:
    {"MISP":<10} https://{hostnames[0]}
    """
    )
