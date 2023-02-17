#!/usr/bin/env bash

set -eu

dir=$(dirname "$(readlink -f "$0")")

"${dir}"/update-plugins.sh

command -v nix > /dev/null && {
  echo "Updating with the local Nix installation..."
  "${dir}/update-impl.sh"
  exit $?
}

toplevel="$(git -C "${dir}" rev-parse --show-toplevel)"
mainroot="$(git -C "${dir}" rev-parse --path-format=absolute --show-toplevel)"
gitdir="$(git -C "${dir}" rev-parse --path-format=absolute --git-common-dir)"
echo "toplevel = ${toplevel}"
echo "mainroot = ${mainroot}"
echo "gitdir = ${gitdir}"

command -v docker > /dev/null && {
  echo "Updating with with a nix docker container..."
  if [ "$toplevel/.git" = "$gitdir" ]; then
    docker run \
      -v "${toplevel}:${toplevel}" \
      nixos/nix "${toplevel}/nix/update-impl.sh"
  else
    docker run \
      -v "${toplevel}:${toplevel}" \
      -v "${gitdir}:${gitdir}" \
      nixos/nix "${toplevel}/nix/update-impl.sh"
  fi
  exit $?
}

echo "Error: This update script requires either nix or docker."
exit 1
