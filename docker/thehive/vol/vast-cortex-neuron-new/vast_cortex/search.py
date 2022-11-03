#!/usr/bin/env python3
# -*- coding: utf-8 -*

import asyncio
import json
import time
import sys
from cortexutils.analyzer import Analyzer
import vast
import cli

class VastAnalyzer(Analyzer):
    def __init__(self):
        Analyzer.__init__(self)
        self.host = self.get_param(
            "config.endpoint", None, "Vast Server endpoint is missing"
        )
        self.max_events = self.get_param(
            "config.max_events", 30, "Max result value is missing"
        )
        self.vastClient = vast.VAST()

    async def run(self):
        print("run")
        # self.vastClient.export(expression = ":addr == 1.2.3.4")
        proc = await vast.CLI().export(max_events=limit).arrow(expression).exec()

if __name__ == "__main__":
    asyncio.run(VastAnalyzer().run())
