#!/usr/bin/env bash

set -e

curl -L https://storage.googleapis.com/tenzir-datasets/M57/suricata.json.zst | unzstd > suricata.json
curl -L https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst | unzstd > zeek-all.log
