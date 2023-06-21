#!/bin/sh
#
# This script installs the latest release of Tenzir.
#

# Treat unset variables as an error when substituting.
set -u

#
# Helper utilities
#

# Only use colors when we have a TTY.
if [ -t 1 ]
then
  escape() { printf "\033[%sm" "$1"; }
else
  escape() { :; }
fi
make_bold() { escape "1;$1"; }
green="$(make_bold 32)"
blue="$(make_bold 34)"
bold="$(make_bold 39)"
normal="$(escape 0)"

# Prints an "action", i.e., something the script is doing.
# Use this to transparently inform the user about the key
# steps this script takes.
action() {
  printf "${blue}==>${bold} %s${normal}\n" "$@"
}

#
# Execution
#

if [ -z "${BASH_VERSION:-}" ]
then
  echo "Bash is required to interpret this script."
  exit 1
fi

# Print welcome banner.
echo "${blue}"
echo "                _____ _____ _   _ ________ ____  "
echo "               |_   _| ____| \ | |__  /_ _|  _ \ "
echo "                 | | |  _| |  \| | / / | || |_) |"
echo "                 | | | |___| |\  |/ /_ | ||  _ < "
echo "                 |_| |_____|_| \_/____|___|_| \\_\\"
echo "${normal}"
echo "                            INSTALLER"
echo

# Detect OS.
action "Identifying platform"
PLATFORM=
OS=$(uname -s)
if [ "${OS}" = "Linux" ]
then
    if [ -f "/etc/debian_version" ]
    then
        PLATFORM=Debian
    elif [ -f "/etc/NIXOS" ]
    then
        PLATFORM=NixOS
    else
        PLATFORM=Linux
    fi
elif [ "${OS}" = "Darwin" ]
then
  PLATFORM=macOS
fi
echo "Found ${PLATFORM}."

# Select appropriate package.
action "Identifying package"
PACKAGE=
if [ "${PLATFORM}" = "Debian" ]
then
  PACKAGE="tenzir-linux-static.deb"
elif [ "${PLATFORM}" = "Linux" ]
then
  PACKAGE="tenzir-linux-static.tar.gz"
elif [ "${PLATFORM}" = "NixOS" ]
then
  echo "Try Tenzir with our ${bold}flake.nix${normal}:"
  echo
  echo "    ${bold}nix run github:tenzir/tenzir/stable${normal}"
  echo
  echo "Install Tenzir by adding" \
    "${bold}github:tenzir/tenzir/stable${normal} to your"
  echo "flake inputs, or use your preferred method to include third-party"
  echo "modules on classic NixOS."
  exit 0
else
  echo "We do not offer pre-built packages for ${PLATFORM}." \
       "Your options:"
  echo
  echo "  1. Use Docker"
  echo "  2. Build from source"
  echo
  echo "Visit ${bold}https://docs.tenzir.com${normal} for further instructions."
  exit 1
fi
echo "Using ${PACKAGE}"

# Download package.
BASE="https://github.com/tenzir/tenzir/releases/latest/download"
URL="${BASE}/${PACKAGE}"
action "Downloading ${URL}"
curl --progress-bar -L "${URL}" -o "${PACKAGE}" || exit 1
echo "Successfully downloaded ${PACKAGE}"

# Get platform config.
PREFIX=/opt/tenzir
CONFIG="${PREFIX}/etc/tenzir/plugins/platform.yaml"
echo
echo "Unlock the full potential of your Tenzir node as follows:"
echo
echo "  1. Go to ${bold}https://app.tenzir.com${normal}"
echo "  2. Sign up for a free account"
echo "  3. Configure a node and download a ${bold}platform.yaml${normal} config"
echo "  4. Move the config to ${bold}${CONFIG}${normal}"
echo
echo "(You can skip these steps, but then only open source features will be"
echo "available. Visit ${bold}https://tenzir.com/pricing${normal} for a" \
  "feature comparison.)"
echo
echo "Press ${green}ENTER${normal} to proceed with the installation."
read -r

# Check for platform configuration.
if ! [ -f "${CONFIG}" ]
then
  OPEN_SOURCE=1
fi
if [ -n "${OPEN_SOURCE}" ]
then
  echo "Could not find platform config at ${bold}${CONFIG}${normal}."
  action "Using open source edition"
fi

# Trigger installation.
action "Installing package into ${PREFIX}"
if [ "${PLATFORM}" = "Debian" ]
then
  action "Installing via dpkg"
  dpkg -i "${PACKAGE}"
  action "Checking node status"
  systemctl status tenzir || exit 1
elif [ "${PLATFORM}" = "Linux" ]
then
  action "Unpacking tarball"
  tar xzf "${PACKAGE}" -C /
fi

# Test the installation.
action "Checking version"
PATH="${PREFIX}:$PATH"
tenzir -q 'version | select version | write json' || exit 1

# Inform about next steps.
action "Providing guidance"
echo "You're all set! Next steps:"
echo
echo "  - Run a pipeline via ${green}tenzir <pipeline>${normal}"
if [ "${PLATFORM}" = "Linux" ]
then
  echo "  - Spawn a node via ${green}tenzir-node${normal}"
fi
if [ -z "${OPEN_SOURCE}" ]
then
  echo "  - Explore your node and manage pipelines at" \
    "${bold}https://app.tenzir.com${normal}"
fi
echo "  - Follow the guides at ${bold}https://docs.tenzir.com${normal}"
echo "  - Swing by our friendly Discord at" \
  "${bold}https://discord.tenzir.com${normal}"
