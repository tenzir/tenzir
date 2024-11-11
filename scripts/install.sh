#!/bin/sh
#
# This script installs the latest release of Tenzir.
#

# Treat unset variables as an error when substituting.
set -u

# Abort on error.
set -e

#
# Helper utilities
#

# Check whether a command exists.
check() {
  command -v "$1" >/dev/null 2>&1
}

beginswith() {
  case $2 in "$1"*) true;; *) false;; esac;
}

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

# Only prompt for user confirmation if we have a TTY.
confirm() {
  if [ -t 1 ]
  then
    echo
    echo "Press ${green}ENTER${normal} to continue."
    read -r response < /dev/tty
  fi
}

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
platform=
os=$(uname -s)
if [ "${os}" = "Linux" ]
then
    if check rpm && [ -n "$(rpm -qa 2>/dev/null)" ]
    then
        platform=RPM
    elif [ -f "/etc/debian_version" ]
    then
        platform=Debian
    elif [ -f "/etc/NIXOS" ]
    then
        platform=NixOS
    else
        platform=Linux
    fi
elif [ "${os}" = "Darwin" ]
then
  platform=macOS
elif [ -z "${os}" ]
then
  echo "Could not identify platform."
  exit 1
fi
echo "Found ${platform}."

# Figure out if we can run privileged.
can_root=
sudo=
if [ "$(id -u)" = 0 ]
then
  can_root=1
  sudo=""
elif check sudo
then
  can_root=1
  sudo="sudo"
elif check doas
then
  can_root=1
  sudo="doas"
fi
if [ "$can_root" != "1" ]
then
  echo "Could not find ${bold}sudo${normal} or ${bold}doas${normal}."
  echo "This installer needs to run commands as the ${bold}root${normal} user."
  echo "Re-run this script as root or set up sudo/doas."
  exit 1
fi

# The caller can supply a package URL with an environment variable. This should
# only be used for testing modifications to the packaging.
if [ -n "${TENZIR_PACKAGE_URL:-}" ]
then
  package_url="${TENZIR_PACKAGE_URL}"
else
  : "${TENZIR_PACKAGE_TAG:=latest}"
  # Select appropriate package.
  action "Identifying package"
  package_url_base="https://storage.googleapis.com/tenzir-dist-public/packages/main"
  if [ "${platform}" = "RPM" ]
  then
    package_url="${package_url_base}/rpm/tenzir-static-${TENZIR_PACKAGE_TAG}.rpm"
  elif [ "${platform}" = "Debian" ]
  then
    package_url="${package_url_base}/debian/tenzir-static-${TENZIR_PACKAGE_TAG}.deb"
  elif [ "${platform}" = "Linux" ]
  then
    package_url="${package_url_base}/tarball/tenzir-static-${TENZIR_PACKAGE_TAG}.gz"
  elif [ "${platform}" = "NixOS" ]
  then
    echo "Try Tenzir with our ${bold}flake.nix${normal}:"
    echo
    echo "    ${bold}nix shell github:tenzir/tenzir/${TENZIR_PACKAGE_TAG} -c tenzir-node${normal}"
    echo
    echo "Install Tenzir by adding" \
      "${bold}github:tenzir/tenzir/${TENZIR_PACKAGE_TAG}${normal} to your"
    echo "flake inputs, or use your preferred method to include third-party"
    echo "modules on classic NixOS."
    exit 0
  elif [ "${platform}" = "macOS" ]
  then
    package_url="${package_url_base}/macOS/tenzir-static-${TENZIR_PACKAGE_TAG}.pkg"
  else
    echo "We do not offer pre-built packages for ${platform}." \
        "Your options:"
    echo
    echo "  1. Use Docker"
    echo "  2. Build from source"
    echo
    echo "Visit ${bold}https://docs.tenzir.com${normal} for further instructions."
    exit 1
  fi
fi
package="$(basename "${package_url}")"
echo "Using ${package}"

# Download package.
tmpdir="$(dirname "$(mktemp -u)")"
action "Downloading ${package_url}"
rm -f "${tmpdir}/${package}"
# Wget does not support the file:// URL scheme.
if check wget && beginswith "${package_url}" "https://"
then
  wget -q --show-progress -O "${tmpdir}/${package}" "${package_url}"
elif check curl
then
  curl --progress-bar -L -o "${tmpdir}/${package}" "${package_url}"
else
  echo "Neither ${bold}wget${normal} nor ${bold}curl${normal}" \
    "found in \$PATH."
  exit 1
fi
echo "Successfully downloaded ${package}"

# Get platform config.
open_source=
prefix=/opt/tenzir
config="${prefix}/etc/tenzir/plugin/platform.yaml"
echo
echo "Unlock the full potential of your Tenzir node as follows:"
echo
echo "  1. Go to ${bold}https://app.tenzir.com${normal}"
echo "  2. Sign up for a free account"
echo "  3. Configure a node and download a ${bold}platform.yaml${normal} config"
echo "  4. Move the config to ${bold}${config}${normal}"
echo
echo "(You can skip these steps, but then only open source features will be"
echo "available. Visit ${bold}https://tenzir.com/pricing${normal} for a" \
  "feature comparison.)"
confirm

# Check for platform configuration.
action "Checking for existence of platform configuration"
if ! [ -f "${config}" ]
then
  echo "Could not find config at ${bold}${config}${normal}."
  action "Using open source feature set"
  open_source=1
else
  echo "Found config at ${bold}${config}${normal}."
  action "Using complete feature set"
fi

# Trigger installation.
action "Installing package into ${prefix}"
if [ "${platform}" = "RPM" ]
then
  cmd1="$sudo yum -y --nogpgcheck localinstall \"${tmpdir}/${package}\""
  cmd2="$sudo systemctl status tenzir-node || [ ! -d /run/systemd/system ]"
  echo "This script is about to run the following commands:"
  echo
  echo "  - ${cmd1}"
  echo "  - ${cmd2}"
  confirm
  action "Installing via yum"
  eval "${cmd1}"
  action "Checking node status"
  eval "${cmd2}"
elif [ "${platform}" = "Debian" ]
then
  cmd1="$sudo apt-get --yes install \"${tmpdir}/${package}\""
  cmd2="$sudo systemctl status tenzir-node || [ ! -d /run/systemd/system ]"
  echo "This script is about to run the following commands:"
  echo
  echo "  - ${cmd1}"
  echo "  - ${cmd2}"
  confirm
  action "Installing via apt-get"
  eval "${cmd1}"
  action "Checking node status"
  eval "${cmd2}"
elif [ "${platform}" = "Linux" ]
then
  cmd1="$sudo tar xzf \"${tmpdir}/${package}\" -C /"
  cmd2="$sudo echo 'export PATH=\$PATH:/opt/tenzir/bin' > /etc/profile.d/tenzir.sh"
  echo "This script is about to run the following command:"
  echo
  echo "  - ${cmd1}"
  echo "  - ${cmd2}"
  confirm
  action "Unpacking tarball"
  eval "${cmd1}"
  action "Adding /opt/tenzir/bin to the system path"
  eval "${cmd2}"
elif [ "${platform}" = "macOS" ]
then
  cmd1="$sudo installer -pkg \"${tmpdir}/${package}\" -target /"
  echo "This script is about to run the following command:"
  echo
  echo "  - ${cmd1}"
  confirm
  action "Installing Tenzir"
  eval "${cmd1}"
fi

# Test the installation.
action "Checking version"
PATH="${prefix}/bin:$PATH"
tenzir -q 'version | write json'

# Inform about next steps.
action "Providing guidance"
echo "You're all set! Next steps:"
echo
echo "  - Ensure that ${bold}${prefix}/bin${normal} is in your \$PATH"
echo "  - Run a pipeline via ${green}tenzir <pipeline>${normal}"
if [ "${platform}" = "Linux" ] || [ "${platform}" = "macOS" ]
then
  echo "  - Spawn a node via ${green}tenzir-node${normal}"
fi
if [ -z "${open_source}" ]
then
  echo "  - Explore your node and manage pipelines at" \
    "${bold}https://app.tenzir.com${normal}"
fi
echo "  - Follow the guides at ${bold}https://docs.tenzir.com${normal}"
echo "  - Swing by our friendly Discord at" \
  "${bold}https://discord.tenzir.com${normal}"
