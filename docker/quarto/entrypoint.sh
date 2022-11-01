#!/usr/bin/env bash

# This script must be executed with elevated priviledges (root)

# Using bindfs, we map the host UID/GID to the local container user defined with
# CONTAINER_USER and CONTAINER_GROUP. The volume to be shared with the host
# should be mounted on /mnt/host and will be made accessible to the container
# user at /host

# The provided commands will be executed in the user space mounted directory /host

set -e

if [[ -z "$CONTAINER_USER" || -z "$CONTAINER_GROUP" ]]; then
    echo 'ERROR: set CONTAINER_USER and CONTAINER_GROUP variables to the target user' >&2
    exit 1
fi

if [[ -z "$HOST_UID" || -z "$HOST_GID" ]]; then
    echo 'ERROR: set HOST_UID=$(id -u) and HOST_GID=$(id -g)' >&2
    exit 1
fi

mkdir /host
# bindfs enables us to maintain permission consistency on the bound host
# the /host volume can be written to safely by the provided user
bindfs --force-user=$CONTAINER_USER --force-group=$CONTAINER_GROUP \
    --create-for-user=$HOST_UID --create-for-group=$HOST_GID \
    --chown-ignore --chgrp-ignore \
    /mnt/host /host

# using Dockerfile WORKDIR creates a sort of race with bindfs so we fall back to
# the good old `cd`
cd /host

# Drop privileges and execute next container command, or 'bash' if not specified.
EXEC_CMD="exec sudo  --preserve-env=PATH --preserve-env --set-home --user=$CONTAINER_USER --"
if [[ $# -gt 0 ]]; then
    $EXEC_CMD "$@"
else
    $EXEC_CMD bash
fi
