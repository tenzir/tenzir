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
echo "${bold}${blue}"
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
echo "Found ${PLATFORM}"

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
  echo "We do not offer pre-built packages for ${bold}${PLATFORM}${normal}." \
       "Your options:"
  echo
  echo "1. Use Docker"
  echo "2. Build from source"
  echo
  echo "See ${bold}https://docs.tenzir.com${normal} for further information."
  exit 1
fi
echo "Using ${PACKAGE}"

# Download package.
BASE="https://github.com/tenzir/tenzir/releases/latest/download"
URL="${BASE}/${PACKAGE}"
action "Downloading ${URL}"
curl --progress-bar -L "${URL}" -o "${PACKAGE}" || exit 1
echo "Successfully downloaded ${PACKAGE}"

# Trigger installation.
PREFIX=/opt/tenzir
action "Installing package into ${PREFIX}"
echo "Press ${bold}ENTER${normal} to continue..."
read -r
if [ "${PLATFORM}" = "Debian" ]
then
  echo "Installing via dpkg"
  dpkg -i "${PACKAGE}"
elif [ "${PLATFORM}" = "Linux" ]
then
  echo "Unpacking tarball"
  tar xzf "${PACKAGE}" -C /
fi

# Test the installation.
action "Verifying installation"
PATH="${PREFIX}:$PATH"
tenzir version || exit 1

# Inform about next steps.
echo
echo "You're all set! Next steps:"
echo "  - Run a pipeline via ${blue}tenzir <pipeline>${normal}"
echo "  - Spawn a node via ${blue}tenzir-node${normal}"
echo "  - Follow the guides at https://docs.tenzir.com/next/user-guides"
echo "  - Swing by our friendly Discord at https://docs.tenzir.com/discord"
