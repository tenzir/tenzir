"""Expose HTTP service with Cloudflare Access"""
import os
import time
from trace import Trace
import requests
import dynaconf
from vast_invoke import Context, Exit, task
from typing import Tuple
from common import (
    FargateService,
    terraform_output,
)

VALIDATORS = [
    dynaconf.Validator("VAST_CLOUDFLARE_ACCOUNT_ID", must_exist=True, ne=""),
    dynaconf.Validator("VAST_CLOUDFLARE_ZONE", must_exist=True, ne=""),
    dynaconf.Validator("VAST_CLOUDFLARE_API_TOKEN", must_exist=True, ne=""),
    dynaconf.Validator("VAST_CLOUDFLARE_EXPOSE", must_exist=True, ne=""),
    dynaconf.Validator("VAST_CLOUDFLARE_AUTHORIZED_EMAILS", must_exist=True, ne=""),
]


def service_outputs(c: Context) -> Tuple[str, str, str]:
    cluster = terraform_output(c, "core-2", "fargate_cluster_name")
    family = terraform_output(c, "cloudflare", "cloudflare_task_family")
    service_name = terraform_output(c, "cloudflare", "cloudflare_service_name")
    return (cluster, service_name, family)


class CloudflareClient:
    """A lightweight Cloudflare API client implementing the few needed endpoints"""

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

    def get_tunnel_config(self, tunnel_id):
        """Get the tunnel config for the provided id (following the syntax rules of config.yaml)"""
        resp = requests.get(
            f"{self.base_url}/accounts/{self.account_id}/cfd_tunnel/{tunnel_id}/configurations",
            headers=self._headers(),
        )
        resp.raise_for_status()
        return resp.json()["result"]["config"]


def list_exposed_urls(c):
    cf_expose = os.environ["VAST_CLOUDFLARE_EXPOSE"]
    try:
        for exp in cf_expose.split(","):
            mod, var = exp.split(".")
            yield terraform_output(c, mod, var)
    except Exception as e:
        if Trace:
            print(e, file=sys.stderr)
        raise Exit(
            f"Could not get urls for VAST_CLOUDFLARE_EXPOSE={os.environ['VAST_CLOUDFLARE_EXPOSE']}"
        )


def display_rules(rules):
    print("Exposing apps:")
    for rule in rules:
        # Don't display the default rule
        if "hostname" in rule:
            print(f"{rule['service']} -> https://{rule['hostname']}")


@task
def config(c):
    """Update the route configurations of the Cloudflare tunnel"""
    tunnel_id = terraform_output(c, "cloudflare", "cloudflare_tunnel_id")
    hostnames = terraform_output(c, "cloudflare", "cloudflare_hostnames").split(",")
    rules = [
        {
            "hostname": m[0],
            "service": m[1],
        }
        for m in zip(hostnames, list_exposed_urls(c))
    ]
    rules = [*rules, {"service": "http_status:404"}]
    CloudflareClient().update_tunnel_config(
        tunnel_id,
        {"config": {"ingress": rules}},
    )
    display_rules(rules)


@task
def list(c):
    """List the route configurations of the Cloudflare tunnel"""
    tunnel_id = terraform_output(c, "cloudflare", "cloudflare_tunnel_id")
    config = CloudflareClient().get_tunnel_config(tunnel_id)
    display_rules(config["ingress"])


@task
def status(c):
    """Get the status of the cloudflare service"""
    print(FargateService(*service_outputs(c)).service_status())


@task
def start(c):
    """Configure and start cloudflared as an AWS Fargate task. Noop if it is already running"""
    FargateService(*service_outputs(c)).start_service()
    # Surprisingly, cloudflared needs to be started when the tunnel is configured,
    # otherwise the configuration is not loaded by the daemon :-\
    time.sleep(5)
    config(c)


@task
def stop(c):
    """Stop the cloudflared instance and service"""
    FargateService(*service_outputs(c)).stop_service()


@task
def restart(c):
    """Stop the running cloudflared task, the service starts a new one"""
    FargateService(*service_outputs(c)).restart_service()
