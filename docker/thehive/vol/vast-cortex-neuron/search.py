#!/usr/bin/env python3
# -*- coding: utf-8 -*

import asyncio
import json
import time
import sys
from cortexutils.analyzer import Analyzer
from pyvast import VAST


class VastAnalyzer(Analyzer):
    def __init__(self):
        # thomas adding a comment to change the failing line no
        print(f"tp;args: {sys.argv}")
        print(f"tp;stdin: {sys.stdin.readlines()}")
        time.sleep(6)
        Analyzer.__init__(self)
        self.host = self.get_param(
            "config.endpoint", None, "Vast Server endpoint is missing"
        )
        self.max_events = self.get_param(
            "config.max_events", 30, "Max result value is missing"
        )
        self.vastClient = VAST(endpoint=self.host)

    async def run(self):
        try:
            if not await self.vastClient.test_connection():
                self.error("Could not connect to VAST Server Endpoint")

            query = ""
            if self.data_type == "ip":
                query = f":addr == {self.get_data()}"
            elif self.data_type == "subnet":
                query = f":addr in {self.get_data()}"
            elif self.data_type in ["hash", "domain"]:
                query = f"{self.get_data()}"

            proc = (
                await self.vastClient.export(max_events=self.max_events)
                .json(query)
                .exec()
            )
            stdout, stderr = await proc.communicate()
            if stdout != b"":
                result = [
                    json.loads(str(item))
                    for item in stdout.decode("ASCII").strip().split("\n")
                ]
            else:
                result = []

            self.report({"values": result})
        except Exception as e:
            error = {"input": self._input, "error": e}
            self.unexpectedError(error)

    def summary(self, raw):
        taxonomies = []
        namespace = "VAST"
        predicate = "Hits"

        valuesCount = len(raw["values"])
        value = f"{valuesCount}"
        if valuesCount > 0:
            level = "suspicious"
        else:
            level = "safe"

        taxonomies.append(self.build_taxonomy(level, namespace, predicate, value))

        return {"taxonomies": taxonomies}


if __name__ == "__main__":
    asyncio.run(VastAnalyzer().run())
