import json

import requests
from vast import VAST
import utils.logging

logger = utils.logging.get(__name__)

# FIXME
import random
import string

# The TheHive app.
class TheHive:
    def __init__(self, vast: VAST):
        self.vast = vast
        self.config = vast.config.apps.thehive

    async def run(self):
        await self.vast.fabric.subscribe("suricata.alert", self._on_alert)

    async def _on_alert(self, alert):
        json_alert = json.loads(alert)
        logger.debug(f"got alert: {json_alert}")
        thehive_alert = {
            "type": "string",
            "source": "string",
            "sourceRef": ''.join(random.choice(string.ascii_letters) for x in range(10)),
            "externalLink": "string",
            "title": "string",
            "description": "string",
            "severity": 1,
            "date": 1640000000000,
            "tags": ["string"],
            "flag": True,
            "tlp": 0,
            "pap": 0,
            "summary": "string",
            "caseTemplate": "string",
            "observables": [
                {
                    "dataType": "ip",
                    "data": json_alert["src_ip"],
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
            ]
        }
        response = requests.post(
            f"{self.config.host}/api/v1/alert",
            files={"_json": json.dumps(thehive_alert)},
            headers={"Authorization": f"Bearer {self.config.key}"},
        )
        logger.debug(response.text)
        response.raise_for_status()

async def start(vast: VAST):
    misp = TheHive(vast)
    await misp.run()
