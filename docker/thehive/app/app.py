import asyncio
import datetime
import hashlib
import json
import os
import time
from typing import Dict, Optional

import aiohttp
import vast.utils.logging as logging
from dateutil.parser import isoparse

from vast import VAST, ExportMode

logger = logging.get("vast.thehive.app")

THEHIVE_ORGADMIN_EMAIL = os.environ["DEFAULT_ORGADMIN_EMAIL"]
THEHIVE_ORGADMIN_PWD = os.environ["DEFAULT_ORGADMIN_PWD"]
THEHIVE_URL = os.environ["THEHIVE_URL"]
BACKFILL_LIMIT = int(os.environ["BACKFILL_LIMIT"])

# An in memory cache of the event refs that where already sent to TheHive This
# helps limiting the amount of unnecessary calls to the API when dealing with
# duplicates
SENT_ALERT_REFS = set()


async def call_thehive(
    path: str,
    payload: Optional[Dict] = None,
) -> str:
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


async def wait_for_thehive(
    path: str,
    timeout: int,
    payload: Optional[Dict] = None,
) -> str:
    """Call thehive repeatedly until timeout"""
    start = time.time()
    while True:
        try:
            return await call_thehive(path, payload)
        except Exception as e:
            if time.time() - start > timeout:
                raise Exception("Timed out trying to reach TheHive")
            logger.debug(e)
            await asyncio.sleep(1)


def suricata2thehive(event: Dict) -> Dict:
    """Convert a Suricata alert event into a TheHive alert"""
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

    thehive_alert = suricata2thehive(alert)
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
    await wait_for_thehive("/api/v1/user/current", 180)
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


async def wait_for_alerts(timeout):
    """Call TheHive listAlert API until the number of alerts is greater than 0"""
    list_query = {"query": [{"_name": "listAlert"}]}
    start = time.time()
    while True:
        try:
            alerts_json = await call_thehive("/api/v1/query", list_query)
            alert_count = len(json.loads(alerts_json))
            if alert_count == 0:
                raise Exception("No alerts in TheHive yet")
            return alert_count
        except Exception as e:
            if time.time() - start > timeout:
                raise Exception("Timed out trying to reach TheHive")
            logger.debug(e)
            await asyncio.sleep(1)


def count_alerts():
    """Wait for alerts in TheHive then print their count"""
    nb_alerts = asyncio.run(wait_for_alerts(180))
    logger.info(f"alert_count={nb_alerts}")
