#!/usr/bin/env bash

# The tag on docker.io the database is created with
OLD_VERSION=$1
# A tag on ghcr.io to validate retro compatibility
NEW_VERSION=$2

set -euxo pipefail

pushd "$(git -C "$(dirname "$(readlink -f "${0}")")" rev-parse --show-toplevel)"

VAST_RUN_FLAGS="-d --pull=always --rm --name vast-regression -e VAST_CONSOLE_VERBOSITY=verbose -v vast-regression:/var/lib/vast/"

# Pull the old version to create a database.
docker run \
  $VAST_RUN_FLAGS \
  docker.io/tenzir/vast:$OLD_VERSION \
  start

sleep 3

docker exec -i vast-regression \
  vast import --blocking suricata \
  < vast/integration/data/suricata/eve.json

docker exec vast-regression \
  vast flush

docker exec vast-regression \
  vast export json '#type == "suricata.alert"' \
  > old.json \
|| docker exec vast-regression \
  vast export json 'where #type == "suricata.alert"' \
  > old.json

docker rm -f vast-regression

# Pull the new version to verify database compatibility.
docker run \
  $VAST_RUN_FLAGS \
  ghcr.io/tenzir/vast:$NEW_VERSION \
  start

sleep 3

docker exec vast-regression \
  vast export json '#type == "suricata.alert"' \
  > new.json \
|| docker exec vast-regression \
  vast export json 'where #type == "suricata.alert"' \
  > new.json

docker rm -f vast-regression
docker volume rm vast-regression

# Compare old and new
diff old.json new.json
