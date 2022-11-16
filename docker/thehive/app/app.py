import asyncio
import datetime
import hashlib
import logging
import os
import time
from typing import Dict

import aiohttp
import vast.utils.logging as logging
from dateutil.parser import isoparse

from vast import VAST, ExportMode

logger = logging.get("vast.thehive.app")

THEHIVE_ORGADMIN_EMAIL = "orgadmin@thehive.local"
THEHIVE_ORGADMIN_PWD = "secret"
THEHIVE_URL = os.environ["THEHIVE_URL"]
BACKFILL_LIMIT = int(os.environ["BACKFILL_LIMIT"])

# An in memory cache of the event refs that where already sent to TheHive This
# helps limiting the amount of unnecessary calls to the API when dealing with
# duplicates
SENT_ALERT_REFS = set()


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
    logger.debug(f"Resp to query on TheHive API {path}: {resp_txt}")
    resp.raise_for_status()
    logger.info(f"Call to TheHive API {path} successful!")
    return resp_txt


async def wait_for_thehive(timeout):
    start = time.time()
    while True:
        try:
            await call_thehive("/api/v1/user/current")
            break
        except Exception as e:
            if time.time() - start > timeout:
                raise Exception("Timed out trying to reach TheHive")
            logger.debug(e)
            time.sleep(1)


def suricata2hive(event: Dict) -> Dict:
    # convert iso into epoch
    sighted_time_iso = event.get("timestamp", datetime.datetime.now().isoformat())
    sighted_time_ms = int(isoparse(sighted_time_iso).timestamp() * 1000)
    start_time_iso = event.get("flow", {}).get("timestamp", sighted_time_iso)
    start_time_ms = int(isoparse(start_time_iso).timestamp() * 1000)
    current_time_ms = int(time.time() * 1000)
    # severity is reversed
    severity = min(5 - event.get("severity", 3), 1)
    alert = event.get("alert", {})
    category = alert.get("category")
    desc = f'{alert.get("signature_id", "No signature ID")}: {alert.get("signature", "No signature")}'
    # A unique identifier of this alert, hashing together the start time and flow id
    src_ref = hashlib.md5(
        f'{start_time_iso}{event.get("flow_id", "")}'.encode()
    ).hexdigest()

    return {
        "type": category,
        "source": "suricata",
        "sourceRef": src_ref,
        "title": "Suricata Alert",
        "description": desc,
        "severity": severity,
        "date": current_time_ms,
        "tags": [],
        "observables": [
            {
                "dataType": "ip",
                "data": event["src_ip"],
                "message": "Source IP",
                "startDate": start_time_ms,
                "tags": [],
                "ioc": False,
                "sighted": True,
                "sightedAt": sighted_time_ms,
                "ignoreSimilarity": False,
            },
            {
                "dataType": "ip",
                "data": event["dest_ip"],
                "message": "Destination IP",
                "startDate": start_time_ms,
                "tags": [],
                "ioc": False,
                "sighted": True,
                "sightedAt": sighted_time_ms,
                "ignoreSimilarity": False,
            },
        ],
    }


async def on_suricata_alert(alert: Dict):
    global SENT_ALERT_REFS
    logger.debug(f"Received alert: {alert}")

    thehive_alert = suricata2hive(alert)
    ref = thehive_alert["sourceRef"]
    if ref in SENT_ALERT_REFS:
        logger.debug(f"Alert with hash {ref} skipped")
        return
    try:
        await call_thehive("/api/v1/alert", thehive_alert)
        logger.debug(f"Alert with hash {ref} ingested")
    except aiohttp.ClientResponseError as e:
        if e.code == 400:
            logger.debug(f"Alert with hash {ref} not ingested (error 400)")
        else:
            raise e
    SENT_ALERT_REFS.add(ref)


async def run_async():
    await VAST.status(60, retry_delay=1)
    await wait_for_thehive(120)
    expr = '#type == "suricata.alert"'
    # We don't use "UNIFIED" to specify a limit on the HISTORICAL backfill
    logger.info("Starting retro filling...")
    hist_iter = VAST.export_rows(expr, ExportMode.HISTORICAL, limit=BACKFILL_LIMIT)
    async for alert in hist_iter:
        await on_suricata_alert(alert)
    logger.info("Starting live forwarding...")
    cont_iter = VAST.export_rows(expr, ExportMode.CONTINUOUS)
    async for alert in cont_iter:
        await on_suricata_alert(alert)


def run():
    logger.info("Starting TheHive app...")
    asyncio.run(run_async())
    logger.info("TheHive app stopped")
