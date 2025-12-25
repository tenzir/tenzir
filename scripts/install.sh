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
  case $2 in "$1"*) true ;; *) false ;; esac
}

# Only use colors when we have a TTY.
if [ -t 1 ]; then
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
  if [ -t 1 ]; then
    echo
    echo "Press ${green}ENTER${normal} to continue."
    read -r response </dev/tty
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

action "Identifying platform"
# Detect ARCH.
arch=$(uname -m)
# Detect OS.
package_format=
os=$(uname -s)
if [ "${os}" = "Linux" ]; then
  if check rpm && [ -n "$(rpm -qa 2>/dev/null)" ]; then
    package_format=RPM
  elif [ -f "/etc/debian_version" ]; then
    package_format=DEB
  elif [ -f "/etc/NIXOS" ]; then
    package_format=NixOS
  else
    package_format=tarball
  fi
elif [ "${os}" = "Darwin" ]; then
  package_format=macOS
elif [ -z "${os}" ]; then
  echo "Could not identify package_format."
  exit 1
fi
platform="${arch}-$(echo "${os}" | tr '[:upper:]' '[:lower:]')"

echo "Found ${platform} (${package_format})."

# Figure out if we can run privileged.
can_root=
sudo=
if [ "$(id -u)" = 0 ]; then
  can_root=1
  sudo=""
elif check sudo; then
  can_root=1
  sudo="sudo"
elif check doas; then
  can_root=1
  sudo="doas"
fi
if [ "$can_root" != "1" ]; then
  echo "Could not find ${bold}sudo${normal} or ${bold}doas${normal}."
  echo "This installer needs to run commands as the ${bold}root${normal} user."
  echo "Re-run this script as root or set up sudo/doas."
  exit 1
fi

# The caller can supply a package URL with an environment variable. This should
# only be used for testing modifications to the packaging.
if [ -n "${TENZIR_PACKAGE_URL:-}" ]; then
  package_url="${TENZIR_PACKAGE_URL}"
else
  : "${TENZIR_PACKAGE_TAG:=latest}"
  # Select appropriate package.
  action "Identifying package"
  package_url_base="https://storage.googleapis.com/tenzir-dist-public/packages/main"
  if [ "${package_format}" = "RPM" ]; then
    package_url="${package_url_base}/rpm/tenzir-${TENZIR_PACKAGE_TAG}-${platform}-static.rpm"
  elif [ "${package_format}" = "DEB" ]; then
    package_url="${package_url_base}/debian/tenzir-${TENZIR_PACKAGE_TAG}-${platform}-static.deb"
  elif [ "${package_format}" = "tarball" ]; then
    package_url="${package_url_base}/tarball/tenzir-${TENZIR_PACKAGE_TAG}-${platform}-static.tar.gz"
  elif [ "${package_format}" = "NixOS" ]; then
    echo "Try Tenzir with our ${bold}flake.nix${normal}:"
    echo
    echo "    ${bold}nix shell github:tenzir/tenzir/${TENZIR_PACKAGE_TAG} -c tenzir-node${normal}"
    echo
    echo "Install Tenzir by adding" \
      "${bold}github:tenzir/tenzir/${TENZIR_PACKAGE_TAG}${normal} to your"
    echo "flake inputs, or use your preferred method to include third-party"
    echo "modules on classic NixOS."
    exit 0
  elif [ "${package_format}" = "macOS" ]; then
    package_url="${package_url_base}/macOS/tenzir-${TENZIR_PACKAGE_TAG}-${platform}-static.pkg"
  else
    echo "We do not offer pre-built packages for ${package_format}." \
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
if check wget && beginswith "${package_url}" "https://"; then
  wget -q --show-progress -O "${tmpdir}/${package}" "${package_url}"
elif check curl; then
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
if ! [ -f "${config}" ]; then
  echo "Could not find config at ${bold}${config}${normal}."
  action "Using open source feature set"
  open_source=1
else
  echo "Found config at ${bold}${config}${normal}."
  action "Using complete feature set"
fi

# Trigger installation.
action "Installing package into ${prefix}"
if [ "${package_format}" = "RPM" ]; then
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
elif [ "${package_format}" = "DEB" ]; then
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
elif [ "${package_format}" = "tarball" ]; then
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
elif [ "${package_format}" = "macOS" ]; then
  cmd1="$sudo installer -pkg \"${tmpdir}/${package}\" -target /"
  echo "This script is about to run the following command:"
  echo
  echo "  - ${cmd1}"
  confirm
  action "Installing Tenzir"
  eval "${cmd1}"
fi

# Configure token if TENZIR_TOKEN is set.
if [ -n "${TENZIR_TOKEN:-}" ]; then
  action "Configuring node token"
  config_file="${prefix}/etc/tenzir/tenzir.yaml"
  config_dir="$(dirname "${config_file}")"

  # Create config directory if needed.
  if [ ! -d "${config_dir}" ]; then
    $sudo mkdir -p "${config_dir}"
  fi

  # Back up existing config with ISO timestamp.
  if [ -f "${config_file}" ]; then
    timestamp="$(date -u +%Y%m%dT%H%M%S)"
    backup_file="${config_file}.${timestamp}"
    action "Backing up existing config to ${backup_file}"
    $sudo cp "${config_file}" "${backup_file}"
  fi

  # Use tenzir to create or update the config with the token.
  # The merge function combines the existing tenzir config (or empty record)
  # with the new token, preserving any other settings.
  if [ -f "${config_file}" ]; then
    # Read existing config, merge in token, write back.
    tql_pipeline="load_file \"${config_file}\"
      read_yaml
      if this.has("tenzir") and tenzir.type_id() == type_id({}) {
        tenzir = { token: \"${TENZIR_TOKEN}\"}, ...tenzir }
      } else {
        tenzir = { token: \"${TENZIR_TOKEN}\"} }
      }
      write_yaml
      "
  else
    # Create new config with token.
    tql_pipeline="from {tenzir: {token: \"${TENZIR_TOKEN}\"}}
      write_yaml"
  fi

  # Write config using tenzir and sudo tee.
  ${prefix}/bin/tenzir -qq "${tql_pipeline}" | $sudo tee "${config_file}" >/dev/null
  echo "Token configured in ${config_file}"

  # Restart the service if it's already running (RPM/DEB packages auto-start it).
  if [ -d /run/systemd/system ] && $sudo systemctl is-active --quiet tenzir-node 2>/dev/null; then
    action "Restarting tenzir-node to apply token configuration"
    $sudo systemctl restart tenzir-node
  fi
fi

# Test the installation.
action "Checking version"
PATH="${prefix}/bin:$PATH"
tenzir -q 'version | select version | write_ndjson'

# Inform about next steps.
action "Providing guidance"
echo "You're all set! Next steps:"
echo
echo "  - Ensure that ${bold}${prefix}/bin${normal} is in your \$PATH"
echo "  - Run a pipeline via ${green}tenzir <pipeline>${normal}"
if [ "${package_format}" = "tarball" ] || [ "${package_format}" = "macOS" ]; then
  echo "  - Spawn a node via ${green}tenzir-node${normal}"
fi
if [ -z "${open_source}" ]; then
  echo "  - Explore your node and manage pipelines at" \
    "${bold}https://app.tenzir.com${normal}"
fi
echo "  - Follow the guides at ${bold}https://docs.tenzir.com${normal}"
echo "  - Swing by our friendly Discord at" \
  "${bold}https://discord.tenzir.com${normal}"
