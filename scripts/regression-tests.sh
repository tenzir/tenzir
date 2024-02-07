#!/usr/bin/env bash

# The tag on docker.io the database is created with
OLD_VERSION=$1
# A tag on ghcr.io to validate retro compatibility
NEW_VERSION=$2

set -euxo pipefail

pushd "$(git -C "$(dirname "$(readlink -f "${0}")")" rev-parse --show-toplevel)"

TENZIR_RUN_FLAGS="-d --pull=always --rm --name tenzir-regression --entrypoint=tenzir-node -e TENZIR_CONSOLE_VERBOSITY=verbose -v tenzir-regression:/var/lib/tenzir/"

# Pull the old version to create a database.
docker run \
  $TENZIR_RUN_FLAGS \
  docker.io/tenzir/tenzir:$OLD_VERSION

sleep 3

docker exec -i tenzir-regression \
  tenzir 'read suricata | import' \
  < tenzir/integration/data/inputs/suricata/eve.json

docker exec tenzir-regression \
  tenzir 'export | where #schema == "suricata.alert" | write json' \
  > old.json

docker rm -f tenzir-regression

# Pull the new version to verify database compatibility.
docker run \
  $TENZIR_RUN_FLAGS \
  ghcr.io/tenzir/tenzir:$NEW_VERSION

sleep 3

docker exec tenzir-regression \
  tenzir 'export | where #schema == "suricata.alert" | write json' \
  > new.json

docker rm -f tenzir-regression
docker volume rm tenzir-regression

# Compare old and new
diff old.json new.json
