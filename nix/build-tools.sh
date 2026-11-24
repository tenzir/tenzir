# shellcheck shell=bash
# Builders can mount site-provided tools here. Developers may select their
# own directory when entering the devshell.
tenzir_build_tools="${TENZIR_BUILD_TOOLS:-/run/tenzir-build-tools/bin}"
if [[ -d "$tenzir_build_tools" ]]; then
  export PATH="$tenzir_build_tools:$PATH"
fi
unset tenzir_build_tools
