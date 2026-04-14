{
  treefmtEval,
  pkgs,
}:
pkgs.writeShellScriptBin "format" ''
  set -euo pipefail
  unset PRJ_ROOT

  treefmt=${treefmtEval.config.package}/bin/treefmt
  config_file=${treefmtEval.config.build.configFile}

  "$treefmt" \
    --config-file="$config_file" \
    --tree-root-file=${treefmtEval.config.projectRootFile} \
    "$@"

  if [ -f contrib/tenzir-plugins/README.md ]; then
    "$treefmt" \
      --config-file="$config_file" \
      --working-dir=contrib/tenzir-plugins \
      --tree-root-file=README.md \
      "$@"
  fi
''
