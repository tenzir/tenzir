#!/usr/bin/env bash

curl -L https://storage.googleapis.com/tenzir-datasets/M57/suricata.tar.zst | tar -x --zstd
curl -L https://storage.googleapis.com/tenzir-datasets/M57/zeek.tar.zst | tar -x --zstd
