from typing import Dict, Tuple
import requests
import os
from os import path
import time
import logging
import sys
import subprocess

logging.getLogger().setLevel(logging.INFO)

CORTEX_HOST = os.getenv("CORTEX_HOST")

CONFIG_LOCATION = "/opt/thp/thehive/conf/application.conf"

# These email/pwd are the defaults and should not be changed!
CORTEX_ADMIN_EMAIL = "admin@thehive.local"
CORTEX_ADMIN_PWD = "secret"


def retry_until_timeout(retried_function, action_name: str, timeout: int):
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
    logging.info(f"{action_name} reached!")


def call_cortex(path: str, payload: Dict, credentials: Tuple[str, str] = None):
    session = requests.Session()
    if credentials is not None:
        session.auth = credentials

    resp = session.post(
        f"{CORTEX_HOST}{path}",
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    logging.debug(f"Resp to POST on Cortex API {path}: {resp.text}")
    resp.raise_for_status()
    logging.info(f"Call to Cortex API {path} successful!")
    return resp.text


def init():
    # The maintenance/migrate initializes the ES database
    # It will fail until ES is "sufficiently" ready, whatever that means
    retry_until_timeout(
        lambda: call_cortex("/api/maintenance/migrate", {}),
        "Cortex server migration",
        120,
    )

    # This user already exists by default but does not have sufficient priviledges to set passwords
    call_cortex(
        "/api/user",
        {
            "login": CORTEX_ADMIN_EMAIL,
            "name": "admin",
            "password": CORTEX_ADMIN_PWD,
            "roles": ["superadmin"],
            "organization": "cortex",
        },
    )

    call_cortex(
        "/api/organization",
        {"name": "Tenzir", "description": "tenzir", "status": "Active"},
        (CORTEX_ADMIN_EMAIL, CORTEX_ADMIN_PWD),
    )

    # We need an orgadmin because the superadmin can only perform user/org
    # related tasks
    call_cortex(
        "/api/user",
        {
            "name": "Tenzir org Admin",
            "roles": ["read", "analyze", "orgadmin"],
            "organization": "Tenzir",
            "login": "orgadmin@thehive.local",
        },
        (CORTEX_ADMIN_EMAIL, CORTEX_ADMIN_PWD),
    )

    # Setting a password is not strictly required but can come in handy to
    # interact with the Cortex UI
    call_cortex(
        "/api/user/orgadmin@thehive.local/password/set",
        {"password": "secret"},
        (CORTEX_ADMIN_EMAIL, CORTEX_ADMIN_PWD),
    )

    # TODO setup analyzer
    # call_cortex(
    #     "/api/organization/analyzer/VAST-Search_1_0",
    #     {
    #         "name": "VAST-Search_1_0",
    #         "configuration": {
    #             "endpoint": "localhost:42000",
    #             "max_events": 40,
    #             "auto_extract_artifacts": False,
    #             "check_tlp": False,
    #             "max_tlp": 2,
    #             "check_pap": False,
    #             "max_pap": 2,
    #         },
    #         "jobCache": 10,
    #         "jobTimeout": 30,
    #     },
    #     ("orgadmin@thehive.local","secret")
    # )

    # The API key is how TheHive interacts with Cortex
    api_key = call_cortex(
        "/api/user/orgadmin@thehive.local/key/renew",
        {},
        (CORTEX_ADMIN_EMAIL, CORTEX_ADMIN_PWD),
    )

    # Use the template to create the actual TheHive config file
    with open("application.conf.template", "r") as config_template_file:
        template_str = config_template_file.read()
        template_str.replace("__CORTEX_URL_PLACEHOLDER__", CORTEX_HOST)
        template_str.replace("__CORTEX_API_KEY_PLACEHOLDER__", api_key)
        with open("/opt/thp/thehive/conf/application.conf", "w") as config_file:
            config_file.write(template_str)


# The init process fills the template and creates the config file
if not path.isfile(CONFIG_LOCATION):
    init()

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
    lambda: requests.get(
        f"http://localhost:9000/index.html", timeout=5
    ).raise_for_status(),
    "TheHive server",
    120,
)

exit(thehive_proc.wait())
