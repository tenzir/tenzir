#!/bin/sh
set -eu
# TQL byte sinks skip empty chunks, so create an actual zero-byte file here.
: >"$FILE_ROOT/empty.yaml.next"
