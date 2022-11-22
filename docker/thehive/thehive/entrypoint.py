import logging
import json
import os
import subprocess
import sys
import time
from os import path
from typing import Dict, Optional, Tuple

import requests

logging.getLogger().setLevel(logging.DEBUG)

CORTEX_URL = os.environ["CORTEX_URL"]
VAST_ENDPOINT = os.environ["VAST_ENDPOINT"]
THEHIVE_URL = "http://localhost:9000"

CONFIG_LOCATION = "/opt/thp/thehive/conf/application.conf"

# Admin email is the default and should not be changed!
CORTEX_ADMIN_EMAIL = "admin@thehive.local"
CORTEX_ADMIN_PWD = os.environ["DEFAULT_ADMIN_PWD"]

# Orgadmin is the user used by TheHive through its API key
# It can also be used to connect to Cortex on the Cortex UI
CORTEX_ORGADMIN_EMAIL = os.environ["DEFAULT_ORGADMIN_EMAIL"]
CORTEX_ORGADMIN_PWD = os.environ["DEFAULT_ORGADMIN_PWD"]

# Admin email is the default and should not be changed!
THEHIVE_ADMIN_EMAIL = "admin@thehive.local"
THEHIVE_ADMIN_DEFAULT_PWD = "secret"
THEHIVE_ADMIN_PWD = os.environ["DEFAULT_ADMIN_PWD"]

# Orgadmin is the user to interact with the cases/alerts
THEHIVE_ORGADMIN_EMAIL = os.environ["DEFAULT_ORGADMIN_EMAIL"]
THEHIVE_ORGADMIN_PWD = os.environ["DEFAULT_ORGADMIN_PWD"]


def retry_until_timeout(retried_function, action_name: str, timeout: int):
    """Execute retried_function repeatedly until it doesn't raise an exception or the timeout expires"""
    start = time.time()
    logging.info(f"Waiting for {action_name}...")
    while True:
        try:
            retried_function()
            break
        except Exception as e:
            logging.debug(f"{action_name} not reachable: {e}")
            if time.time() - start > timeout:
                raise Exception(f"Timeout {timeout} exceeded for {action_name}")
            time.sleep(1)


def call_api(
    url: str,
    path: str,
    payload: Dict,
    api_name: str,
    credentials: Optional[Tuple[str, str]] = None,
):
    """Call a JSON endpoint with optional basic auth"""
    session = requests.Session()
    if credentials is not None:
        session.auth = credentials

    resp = session.post(
        f"{url}{path}",
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    logging.debug(f"Resp to POST on {api_name} API {path}: {resp.text}")
    resp.raise_for_status()
    logging.info(f"Call to {api_name} API {path} successful!")
    return resp.text


def call_cortex(
    path: str, payload: Dict, credentials: Optional[Tuple[str, str]] = None
):
    """Call the Cortex API (path should start with /)"""
    return call_api(CORTEX_URL, path, payload, "Cortex", credentials)


def call_thehive(
    path: str, payload: Dict, credentials: Optional[Tuple[str, str]] = None
):
    """Call the TheHive API (path should start with /)"""
    return call_api(THEHIVE_URL, path, payload, "TheHive", credentials)


def init_cortex():
    """Configure Cortex and copy the obtained API key to TheHive config"""

    # Note: all calls to cortex must be retried because of consitency issues

    # The maintenance/migrate initializes the ES database
    # It will fail until ES is "sufficiently" ready, whatever that means
    call = lambda: call_cortex("/api/maintenance/migrate", {})
    retry_until_timeout(call, "Cortex server migration", 120)

    call = lambda: call_cortex(
        "/api/user",
        {
            "login": CORTEX_ADMIN_EMAIL,
            "name": "admin",
            "password": CORTEX_ADMIN_PWD,
            "roles": ["superadmin"],
            "organization": "cortex",
        },
    )
    retry_until_timeout(call, "Cortex upgrade admin to superadmin", 120)

    call = lambda: call_cortex(
        "/api/organization",
        {
            "name": "Tenzir",
            "description": "Answer the toughest questions in cyber security",
            "status": "Active",
        },
        (CORTEX_ADMIN_EMAIL, CORTEX_ADMIN_PWD),
    )
    retry_until_timeout(call, "Cortex create Tenzir org", 30)

    # We need an orgadmin because the superadmin can only perform user/org
    # related tasks
    call = lambda: call_cortex(
        "/api/user",
        {
            "name": "Tenzir org Admin",
            "roles": ["read", "analyze", "orgadmin"],
            "organization": "Tenzir",
            "login": CORTEX_ORGADMIN_EMAIL,
        },
        (CORTEX_ADMIN_EMAIL, CORTEX_ADMIN_PWD),
    )
    retry_until_timeout(call, "Cortex create orgadmin", 30)

    # Setting a password is not strictly required but can come in handy to
    # interact with the Cortex UI
    call = lambda: call_cortex(
        f"/api/user/{CORTEX_ORGADMIN_EMAIL}/password/set",
        {"password": CORTEX_ORGADMIN_PWD},
        (CORTEX_ADMIN_EMAIL, CORTEX_ADMIN_PWD),
    )
    retry_until_timeout(call, "Cortex set orgadmin password", 30)

    call = lambda: call_cortex(
        "/api/organization/analyzer/VAST-Search_1_0",
        {
            "name": "VAST-Search_1_0",
            "configuration": {
                "endpoint": VAST_ENDPOINT,
                "max_events": 40,
                "auto_extract_artifacts": False,
                "check_tlp": False,
                "max_tlp": 2,
                "check_pap": False,
                "max_pap": 2,
            },
            "jobCache": 10,
            "jobTimeout": 30,
        },
        (CORTEX_ORGADMIN_EMAIL, CORTEX_ORGADMIN_PWD),
    )
    retry_until_timeout(call, "Cortex create analyzer", 30)

    # The API key is how TheHive interacts with Cortex
    api_key = call_cortex(
        f"/api/user/{CORTEX_ORGADMIN_EMAIL}/key/renew",
        {},
        (CORTEX_ADMIN_EMAIL, CORTEX_ADMIN_PWD),
    )

    # Use the template to create the actual TheHive config file
    with open("application.conf.template", "r") as config_template_file:
        template_str = config_template_file.read()
        template_str = template_str.replace("__CORTEX_URL_PLACEHOLDER__", CORTEX_URL)
        template_str = template_str.replace("__CORTEX_API_KEY_PLACEHOLDER__", api_key)
        with open(CONFIG_LOCATION, "w") as config_file:
            config_file.write(template_str)


def init_thehive():
    """Init the org/users in TheHive"""

    result = call_thehive(
        "/api/v1/query",
        {"query": [{"_name": "listUser"}]},
        (THEHIVE_ADMIN_EMAIL, THEHIVE_ADMIN_DEFAULT_PWD),
    )

    admin_id = json.loads(result)[0]["_id"]
    call_thehive(
        f"/api/v1/user/{admin_id}/password/set",
        {"password": THEHIVE_ADMIN_PWD},
        (THEHIVE_ADMIN_EMAIL, THEHIVE_ADMIN_DEFAULT_PWD),
    )

    call_thehive(
        "/api/v0/organisation",
        {
            "description": "Answer the toughest questions in cyber security",
            "name": "Tenzir",
        },
        (THEHIVE_ADMIN_EMAIL, THEHIVE_ADMIN_PWD),
    )

    call_thehive(
        "/api/v1/user",
        {
            "login": THEHIVE_ORGADMIN_EMAIL,
            "name": "Org Admin",
            "organisation": "Tenzir",
            "profile": "org-admin",
            "email": THEHIVE_ORGADMIN_EMAIL,
            "password": THEHIVE_ORGADMIN_PWD,
        },
        (THEHIVE_ADMIN_EMAIL, THEHIVE_ADMIN_PWD),
    )


if __name__ == "__main__":
    # The init process fills the template and creates the config file
    # If the config file is not present, it means init wasn't executed yet
    is_init = not path.isfile(CONFIG_LOCATION)

    if is_init:
        init_cortex()

    thehive_proc = subprocess.Popen(
        [
            "/opt/thehive/entrypoint",
            "--no-config",
            "--config-file",
            CONFIG_LOCATION,
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )

    retry_until_timeout(
        lambda: requests.get(f"{THEHIVE_URL}/index.html", timeout=5).raise_for_status(),
        "TheHive server",
        120,
    )

    if is_init:
        init_thehive()

    exit(thehive_proc.wait())
