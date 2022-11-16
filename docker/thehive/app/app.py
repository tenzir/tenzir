import asyncio
import json
import logging
import os
import random
import string
import time
import datetime
from dateutil.parser import isoparse
from typing import Any, Callable, Coroutine, Dict

import aiohttp
import vast.utils.logging

import vast

THEHIVE_ORGADMIN_EMAIL = "orgadmin@thehive.local"
THEHIVE_ORGADMIN_PWD = "secret"
THEHIVE_URL = os.environ["THEHIVE_URL"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


async def call_thehive(
    path: str,
    payload: Dict | None = None,
):
    """Call a TheHive endpoint with basic auth"""
    path = f"{THEHIVE_URL}{path}"
    auth = aiohttp.BasicAuth(THEHIVE_ORGADMIN_EMAIL, THEHIVE_ORGADMIN_PWD)
    async with aiohttp.ClientSession() as session:
        if payload is None:
            resp = await session.get(path, auth=auth)
        else:
            resp = await session.post(path, json=payload, auth=auth)

    resp_txt = await resp.text()
    logging.debug(f"Resp to query on TheHive API {path}: {resp_txt}")
    resp.raise_for_status()
    logging.info(f"Call to TheHive API {path} successful!")
    return resp_txt


async def wait_for_vast():
    while True:
        proc = await vast.CLI().status().exec()
        _, stderr = await proc.communicate()
        if proc.returncode == 0:
            break
        else:
            logger.debug(stderr.decode())
            time.sleep(1)


async def wait_for_thehive():
    while True:
        try:
            await call_thehive("/api/v1/user/current")
            break
        except Exception as e:
            logger.debug(e)
            time.sleep(1)


async def query(
    callback: Callable[[Dict], Coroutine[Any, Any, None]], limit=0, continuous=False
):
    args = {}
    if continuous:
        args["continuous"] = True
    if limit > 0:
        args["max_events"] = limit
    proc = await vast.CLI().export(**args).json('#type == "suricata.alert"').exec()
    while True:
        if proc.stdout.at_eof():
            break
        line = await proc.stdout.readline()
        if line.strip() == b"":
            continue
        await callback(json.loads(line))

    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.error(stderr.decode())


async def on_suricata_alert(event: Dict):
    logger.debug(f"got alert: {event}")
    ts_iso = event.get("timestamp", datetime.datetime.now().isoformat())
    ts_ms = int(isoparse(ts_iso).timestamp() * 1000)
    thehive_alert = {
        "type": "string",
        "source": "suricata",
        "sourceRef": "".join(random.choice(string.ascii_letters) for x in range(10)),
        "externalLink": "string",
        "title": "string",
        "description": "string",
        "severity": 1,
        "date": ts_ms,
        "tags": ["string"],
        "flag": True,
        "tlp": 0,
        "pap": 0,
        "summary": "string",
        "caseTemplate": "string",
        "observables": [
            {
                "dataType": "ip",
                "data": event["src_ip"],
                "message": "string",
                "startDate": 1640000000000,
                "attachment": {
                    "name": "string",
                    "contentType": "string",
                    "id": "string",
                },
                "tlp": 0,
                "pap": 0,
                "tags": ["string"],
                "ioc": True,
                "sighted": True,
                "sightedAt": 1640000000000,
                "ignoreSimilarity": True,
                "isZip": True,
                "zipPassword": "string",
            }
        ],
    }
    response = await call_thehive("/api/v1/alert", thehive_alert)
    logger.debug(response)


async def run_async():
    await wait_for_vast()
    await wait_for_thehive()
    logger.info("Starting retro filling...")
    await query(on_suricata_alert, limit=100)
    logger.info("Starting live forwarding...")
    await query(on_suricata_alert, continuous=True)


def run():
    logger.info("Starting TheHive app...")
    asyncio.run(run_async())
    logger.info("TheHive app stopped")
