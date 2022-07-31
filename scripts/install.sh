#!/bin/bash
#
# This script installs VAST in /opt/vast.
#
# We borrow heavily scaffolding from the Homebrew installation script from
# https://github.com/Homebrew/install.
#
# TODO: add Homebrew BSD 2-clause license to SBOM.
#
#/bin/bash -c "$(curl -fsSL https://vast.io/install.sh)"

set -u

abort() {
  printf "%s\n" "$@" >&2
  exit 1
}

# Fail fast with a concise message when not using bash
# Single brackets are needed here for POSIX compatibility
# shellcheck disable=SC2292
if [ -z "${BASH_VERSION:-}" ]
then
  abort "Bash is required to interpret this script."
fi

# Check if script is run with force-interactive mode in CI
if [[ -n "${CI-}" && -n "${INTERACTIVE-}" ]]
then
  abort "Cannot run force-interactive mode in CI."
fi

# Check if both `INTERACTIVE` and `NONINTERACTIVE` are set
# Always use single-quoted strings with `exp` expressions
# shellcheck disable=SC2016
if [[ -n "${INTERACTIVE-}" && -n "${NONINTERACTIVE-}" ]]
then
  abort 'Both `$INTERACTIVE` and `$NONINTERACTIVE` are set. Please unset at least one variable and try again.'
fi

if [[ -t 1 ]]
then
  tty_escape() { printf "\033[%sm" "$1"; }
else
  tty_escape() { :; }
fi
tty_mkbold() { tty_escape "1;$1"; }
tty_underline="$(tty_escape "4;39")"
tty_blue="$(tty_mkbold 34)"
tty_red="$(tty_mkbold 31)"
tty_bold="$(tty_mkbold 39)"
tty_reset="$(tty_escape 0)"

shell_join() {
  local arg
  printf "%s" "$1"
  shift
  for arg in "$@"
  do
    printf " "
    printf "%s" "${arg// /\ }"
  done
}

chomp() {
  printf "%s" "${1/"$'\n'"/}"
}

ohai() {
  printf "${tty_blue}==>${tty_bold} %s${tty_reset}\n" "$(shell_join "$@")"
}

warn() {
  printf "${tty_red}Warning${tty_reset}: %s\n" "$(chomp "$1")"
}

# Check if script is run non-interactively (e.g. CI)
# If it is run non-interactively we should not prompt for passwords.
# Always use single-quoted strings with `exp` expressions
# shellcheck disable=SC2016
if [[ -z "${NONINTERACTIVE-}" ]]
then
  if [[ -n "${CI-}" ]]
  then
    warn 'Running in non-interactive mode because `$CI` is set.'
    NONINTERACTIVE=1
  elif [[ ! -t 0 ]]
  then
    if [[ -z "${INTERACTIVE-}" ]]
    then
      warn 'Running in non-interactive mode because `stdin` is not a TTY.'
      NONINTERACTIVE=1
    else
      warn 'Running in interactive mode despite `stdin` not being a TTY because `$INTERACTIVE` is set.'
    fi
  fi
else
  ohai 'Running in non-interactive mode because `$NONINTERACTIVE` is set.'
fi

# USER isn't always set so provide a fall back for the installer and subprocesses.
if [[ -z "${USER-}" ]]
then
  USER="$(chomp "$(id -un)")"
  export USER
fi

# First check OS.
OS="$(uname)"
if [[ "${OS}" == "Linux" ]]
then
  VAST_ON_LINUX=1
elif [[ "${OS}" == "Darwin" ]]
then
  VAST_ON_MACOS=1
else
  abort "VAST only runs on macOS and Linux."
fi

VAST_PREFIX="/opt/vast"
VAST_REMOTE="https://github.com/tenzir/vast"
if [[ -n "${VAST_ON_MACOS}" ]]
then
  UNAME_MACHINE="$(/usr/bin/uname -m)"
  STAT_PRINTF=("stat" "-f")
  PERMISSION_FORMAT="%A"
  CHOWN=("/usr/sbin/chown")
  CHGRP=("/usr/bin/chgrp")
  GROUP="admin"
  TOUCH=("/usr/bin/touch")
  INSTALL=("/usr/bin/install" -d -o "root" -g "wheel" -m "0755")
else
  UNAME_MACHINE="$(uname -m)"
  STAT_PRINTF=("stat" "--printf")
  PERMISSION_FORMAT="%a"
  CHOWN=("/bin/chown")
  CHGRP=("/bin/chgrp")
  GROUP="$(id -gn)"
  TOUCH=("/bin/touch")
  INSTALL=("/usr/bin/install" -d -o "${USER}" -g "${GROUP}" -m "0755")
fi
CHMOD=("/bin/chmod")
MKDIR=("/bin/mkdir" "-p")

# TODO: bump version when new macOS is released or announced
MACOS_NEWEST_UNSUPPORTED="13.0"
# TODO: bump version when new macOS is released
MACOS_OLDEST_SUPPORTED="10.15"

REQUIRED_CURL_VERSION=7.41.0
REQUIRED_GIT_VERSION=2.7.0

unset HAVE_SUDO_ACCESS # unset this from the environment

have_sudo_access() {
  if [[ ! -x "/usr/bin/sudo" ]]
  then
    return 1
  fi
  local -a SUDO=("/usr/bin/sudo")
  if [[ -n "${SUDO_ASKPASS-}" ]]
  then
    SUDO+=("-A")
  elif [[ -n "${NONINTERACTIVE-}" ]]
  then
    SUDO+=("-n")
  fi
  if [[ -z "${HAVE_SUDO_ACCESS-}" ]]
  then
    if [[ -n "${NONINTERACTIVE-}" ]]
    then
      "${SUDO[@]}" -l mkdir &>/dev/null
    else
      "${SUDO[@]}" -v && "${SUDO[@]}" -l mkdir &>/dev/null
    fi
    HAVE_SUDO_ACCESS="$?"
  fi
  if [[ -z "${VAST_ON_LINUX-}" ]] && [[ "${HAVE_SUDO_ACCESS}" -ne 0 ]]
  then
    abort "Need sudo access on macOS (e.g. the user ${USER} needs to be an Administrator)!"
  fi
  return "${HAVE_SUDO_ACCESS}"
}

execute() {
  if ! "$@"
  then
    abort "$(printf "Failed during: %s" "$(shell_join "$@")")"
  fi
}

execute_sudo() {
  local -a args=("$@")
  if have_sudo_access
  then
    if [[ -n "${SUDO_ASKPASS-}" ]]
    then
      args=("-A" "${args[@]}")
    fi
    ohai "/usr/bin/sudo" "${args[@]}"
    execute "/usr/bin/sudo" "${args[@]}"
  else
    ohai "${args[@]}"
    execute "${args[@]}"
  fi
}

getc() {
  local save_state
  save_state="$(/bin/stty -g)"
  /bin/stty raw -echo
  IFS='' read -r -n 1 -d '' "$@"
  /bin/stty "${save_state}"
}

ring_bell() {
  # Use the shell's audible bell.
  if [[ -t 1 ]]
  then
    printf "\a"
  fi
}

wait_for_user() {
  local c
  echo
  echo "Press ${tty_bold}RETURN${tty_reset}/${tty_bold}ENTER${tty_reset} to continue or any other key to abort:"
  getc c
  # we test for \r and \n because some stuff does \r instead
  if ! [[ "${c}" == $'\r' || "${c}" == $'\n' ]]
  then
    exit 1
  fi
}

major_minor() {
  echo "${1%%.*}.$(
    x="${1#*.}"
    echo "${x%%.*}"
  )"
}

version_gt() {
  [[ "${1%.*}" -gt "${2%.*}" ]] || [[ "${1%.*}" -eq "${2%.*}" && "${1#*.}" -gt "${2#*.}" ]]
}
version_ge() {
  [[ "${1%.*}" -gt "${2%.*}" ]] || [[ "${1%.*}" -eq "${2%.*}" && "${1#*.}" -ge "${2#*.}" ]]
}
version_lt() {
  [[ "${1%.*}" -lt "${2%.*}" ]] || [[ "${1%.*}" -eq "${2%.*}" && "${1#*.}" -lt "${2#*.}" ]]
}

should_install_command_line_tools() {
  if [[ -n "${VAST_ON_LINUX-}" ]]
  then
    return 1
  fi
  if version_gt "${macos_version}" "10.13"
  then
    ! [[ -e "/Library/Developer/CommandLineTools/usr/bin/git" ]]
  else
    ! [[ -e "/Library/Developer/CommandLineTools/usr/bin/git" ]] ||
      ! [[ -e "/usr/include/iconv.h" ]]
  fi
}

# Search for the given executable in PATH (avoids a dependency on the `which` command)
which() {
  # Alias to Bash built-in command `type -P`
  type -P "$@"
}

# Search PATH for the specified program that satisfies VAST requirements
# function which is set above
# shellcheck disable=SC2230
find_tool() {
  if [[ $# -ne 1 ]]
  then
    return 1
  fi
  local executable
  while read -r executable
  do
    if "test_$1" "${executable}"
    then
      echo "${executable}"
      break
    fi
  done < <(which -a "$1")
}

# Invalidate sudo timestamp before exiting (if it wasn't active before).
if [[ -x /usr/bin/sudo ]] && ! /usr/bin/sudo -n -v 2>/dev/null
then
  trap '/usr/bin/sudo -k' EXIT
fi

####################################################################### script
if ! command -v git >/dev/null
then
  abort "$(
    cat <<EOABORT
You must install Git before installing VAST. See:
  ${tty_underline}https://vast.io/docs/setup-vast${tty_reset}
EOABORT
  )"
elif [[ -n "${VAST_ON_LINUX-}" ]]
then
  USABLE_GIT="$(find_tool git)"
  if [[ -z "${USABLE_GIT}" ]]
  then
    abort "$(
      cat <<EOABORT
The version of Git that was found does not satisfy requirements for VAST.
Please install Git ${REQUIRED_GIT_VERSION} or newer and add it to your PATH.
EOABORT
    )"
  elif [[ "${USABLE_GIT}" != /usr/bin/git ]]
  then
    export VAST_GIT_PATH="${USABLE_GIT}"
    ohai "Found Git: ${VAST_GIT_PATH}"
  fi
fi

if ! command -v curl >/dev/null
then
  abort "You must install cURL to download VAST."
fi

# shellcheck disable=SC2016
ohai 'Checking for `sudo` access (which may request your password)...'

if [[ -z "${VAST_ON_LINUX-}" ]]
then
  have_sudo_access
else
  if ! [[ -w "${VAST_PREFIX}" ]] && [[ -n "${NONINTERACTIVE-}" ]] && ! have_sudo_access
  then
    abort "Insufficient permissions to install VAST to \"${VAST_PREFIX}\"."
  else
    trap exit SIGINT
    if ! /usr/bin/sudo -n -v &>/dev/null
    then
      ohai "Select a VAST installation directory:"
      echo "- ${tty_bold}Enter your password${tty_reset} to install to ${tty_underline}${VAST_PREFIX}${tty_reset} (${tty_bold}recommended${tty_reset})"
      echo "- ${tty_bold}Press Control-D${tty_reset} to install to ${tty_underline}XXX${tty_reset}"
      echo "- ${tty_bold}Press Control-C${tty_reset} to cancel installation"
    fi
    trap - SIGINT
  fi
fi

if [[ "${EUID:-${UID}}" == "0" ]]
then
  # Allow Azure Pipelines/GitHub Actions/Docker/Concourse/Kubernetes to do
  # everything as root (as it's normal there)
  if ! [[ -f /proc/1/cgroup ]] ||
     ! grep -E "azpl_job|actions_job|docker|garden|kubepods" -q /proc/1/cgroup
  then
    abort "Don't run this as root!"
  fi
fi

if [[ -d "${VAST_PREFIX}" && ! -x "${VAST_PREFIX}" ]]
then
  abort "$(
    cat <<EOABORT
The VAST prefix ${tty_underline}${VAST_PREFIX}${tty_reset} exists but is not searchable.
If this is not intentional, please restore the default permissions and
try running the installer again:
    sudo chmod 775 ${VAST_PREFIX}
EOABORT
  )"
fi

if [[ -z "${VAST_ON_LINUX-}" ]]
then
  # On macOS, support 64-bit Intel and ARM
  if [[ "${UNAME_MACHINE}" != "arm64" ]] && [[ "${UNAME_MACHINE}" != "x86_64" ]]
  then
    abort "VAST is only supported on Intel and ARM processors!"
  fi
else
  # On Linux, support only 64-bit Intel
  if [[ "${UNAME_MACHINE}" == "aarch64" ]]
  then
    abort "$(
      cat <<EOABORT
VAST on Linux is not supported on ARM processors.
You can try an alternate installation method instead:
  ${tty_underline}https://vast.io/docs/setup-vast${tty_reset}
EOABORT
    )"
  elif [[ "${UNAME_MACHINE}" != "x86_64" ]]
  then
    abort "VAST on Linux is only supported on Intel processors!"
  fi
fi

if [[ -z "${VAST_ON_LINUX-}" ]]
then
  macos_version="$(major_minor "$(/usr/bin/sw_vers -productVersion)")"
  if version_lt "${macos_version}" "10.7"
  then
    abort "Your Mac OS X version is too old."
  elif version_lt "${macos_version}" "10.11"
  then
    abort "Your OS X version is too old."
  elif version_ge "${macos_version}" "${MACOS_NEWEST_UNSUPPORTED}" ||
       version_lt "${macos_version}" "${MACOS_OLDEST_SUPPORTED}"
  then
    who="We"
    what=""
    if version_ge "${macos_version}" "${MACOS_NEWEST_UNSUPPORTED}"
    then
      what="pre-release version"
    else
      who+=" (and Apple)"
      what="old version"
    fi
    ohai "You are using macOS ${macos_version}."
    ohai "${who} do not provide support for this ${what}."
  fi
fi

ohai "This script will install:"
echo "${VAST_PREFIX}/bin/vast"
echo "${VAST_PREFIX}/share/doc/vast"
echo "${VAST_PREFIX}/share/man/man1/vast.1"

if should_install_command_line_tools
then
  ohai "The Xcode Command Line Tools will be installed."
fi

if [[ -z "${NONINTERACTIVE-}" ]]
then
  ring_bell
  wait_for_user
fi

if ! [[ -d "${VAST_PREFIX}" ]]
then
  execute_sudo "${INSTALL[@]}" "${VAST_PREFIX}"
fi

if ! [[ -d "${VAST_REPOSITORY}" ]]
then
  execute_sudo "${MKDIR[@]}" "${VAST_REPOSITORY}"
fi
execute_sudo "${CHOWN[@]}" "-R" "${USER}:${GROUP}" "${VAST_REPOSITORY}"

if should_install_command_line_tools && version_ge "${macos_version}" "10.13"
then
  ohai "Searching online for the Command Line Tools"
  # This temporary file prompts the 'softwareupdate' utility to list the Command Line Tools
  clt_placeholder="/tmp/.com.apple.dt.CommandLineTools.installondemand.in-progress"
  execute_sudo "${TOUCH[@]}" "${clt_placeholder}"
  clt_label_command="/usr/sbin/softwareupdate -l |
                      grep -B 1 -E 'Command Line Tools' |
                      awk -F'*' '/^ *\\*/ {print \$2}' |
                      sed -e 's/^ *Label: //' -e 's/^ *//' |
                      sort -V |
                      tail -n1"
  clt_label="$(chomp "$(/bin/bash -c "${clt_label_command}")")"
  if [[ -n "${clt_label}" ]]
  then
    ohai "Installing ${clt_label}"
    execute_sudo "/usr/sbin/softwareupdate" "-i" "${clt_label}"
    execute_sudo "/usr/bin/xcode-select" "--switch" "/Library/Developer/CommandLineTools"
  fi
  execute_sudo "/bin/rm" "-f" "${clt_placeholder}"
fi

# Headless install may have failed, so fallback to original 'xcode-select' method
if should_install_command_line_tools && test -t 0
then
  ohai "Installing the Command Line Tools (expect a GUI popup):"
  execute_sudo "/usr/bin/xcode-select" "--install"
  echo "Press any key when the installation has completed."
  getc
  execute_sudo "/usr/bin/xcode-select" "--switch" "/Library/Developer/CommandLineTools"
fi

if [[ -z "${VAST_ON_LINUX-}" ]] && ! output="$(/usr/bin/xcrun clang 2>&1)" && [[ "${output}" == *"license"* ]]
then
  abort "$(
    cat <<EOABORT
You have not agreed to the Xcode license.
Before running the installer again please agree to the license by opening
Xcode.app or running:
    sudo xcodebuild -license
EOABORT
  )"
fi

cd "${VAST_PREFIX}" || return
if [[ -n "${VAST_ON_LINUX-}" ]]
then
  (
    ohai "Downloading VAST static build..."
    mkdir src
    curl -sSOL --output-dir src https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz
    ohai "Unpacking tarball..."
    tar xzf src/*.tar.gz
  ) || exit 1
  cat <<EOS
  For more information about VAST on Linux, see:
    ${tty_underline}https://vast.io/docs/setup-vast/install/linux${tty_reset}
EOS
elif [[ -n "${VAST_ON_MACOS-}" ]]
then
  (
    ohai "Cloning VAST repository..."
    mkdir src
    git clone --recursive https://github.com/tenzir/vast src/vast
    ohai "Building VAST..."
    cd src/vast
    git checkout stable
    cmake -B build -DCMAKE_BUILD_TYPE=Release
    cmake --build build all
    ohai "Running unit tests..."
    ctest --test-dir build
    cmake --install --prefix "${VAST_PREFIX}"
  ) || exit 1
  cat <<EOS
  For more information about VAST on macOS, see:
    ${tty_underline}https://vast.io/docs/setup-vast/install/macos${tty_reset}
EOS
fi

if [[ ":${PATH}:" != *":${VAST_PREFIX}/bin:"* ]]
then
  warn "${VAST_PREFIX}/bin is not in your PATH.
  Instructions on how to configure your shell for VAST
  can be found in the 'Next steps' section below."
fi

ohai "Installation successful!"
echo

ring_bell

shellenv() {
  case "$(/bin/ps -p "${PPID}" -c -o comm=)" in
    fish | -fish)
      echo "set -gx VAST_PREFIX \"${VAST_PREFIX}\";"
      echo "set -q PATH; or set PATH ''; set -gx PATH \"${VAST_PREFIX}/bin\" \"${VAST_PREFIX}/sbin\" \$PATH;"
      echo "set -q MANPATH; or set MANPATH ''; set -gx MANPATH \"${VAST_PREFIX}/share/man\" \$MANPATH;"
      ;;
    csh | -csh | tcsh | -tcsh)
      echo "setenv VAST_PREFIX ${VAST_PREFIX};"
      echo "setenv PATH ${VAST_PREFIX}/bin:${VAST_PREFIX}/sbin:\$PATH;"
      echo "setenv MANPATH ${VAST_PREFIX}/share/man\`[ \${?MANPATH} == 1 ] && echo \":\${MANPATH}\"\`:;"
      ;;
    pwsh | -pwsh | pwsh-preview | -pwsh-preview)
      echo "[System.Environment]::SetEnvironmentVariable('VAST_PREFIX','${VAST_PREFIX}',[System.EnvironmentVariableTarget]::Process)"
      echo "[System.Environment]::SetEnvironmentVariable('PATH',\$('${VAST_PREFIX}/bin:${VAST_PREFIX}/sbin:'+\$ENV:PATH),[System.EnvironmentVariableTarget]::Process)"
      echo "[System.Environment]::SetEnvironmentVariable('MANPATH',\$('${VAST_PREFIX}/share/man'+\$(if(\${ENV:MANPATH}){':'+\${ENV:MANPATH}})+':'),[System.EnvironmentVariableTarget]::Process)"
      ;;
    *)
      echo "export VAST_PREFIX=\"${VAST_PREFIX}\";"
      echo "export PATH=\"${VAST_PREFIX}/bin:${VAST_PREFIX}/sbin\${PATH+:\$PATH}\";"
      echo "export MANPATH=\"${VAST_PREFIX}/share/man\${MANPATH+:\$MANPATH}:\";"
      ;;
  esac
}

ohai "Next steps:"
case "${SHELL}" in
  */bash*)
    if [[ -r "${HOME}/.bash_profile" ]]
    then
      shell_profile="${HOME}/.bash_profile"
    else
      shell_profile="${HOME}/.profile"
    fi
    ;;
  */zsh*)
    shell_profile="${HOME}/.zprofile"
    ;;
  *)
    shell_profile="${HOME}/.profile"
    ;;
esac

# `which` is a shell function defined above.
# shellcheck disable=SC2230
if [[ "$(which vast)" != "${VAST_PREFIX}/bin/vast" ]]
then
  echo "- Add these commands to your ${tty_bold}${shell_profile}${tty_reset}:"
  printf "%s" shellenv
fi

cat <<EOS
- Run ${tty_bold}vast help${tty_reset} to get started
- Further documentation:
    ${tty_underline}https://vast.io/docs${tty_reset}

EOS
