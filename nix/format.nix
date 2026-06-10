{
  treefmtEval,
  pkgs,
}:
pkgs.writeShellScriptBin "format" ''
  set -euo pipefail
  unset PRJ_ROOT

  exec ${treefmtEval.config.package}/bin/treefmt \
    --config-file=${treefmtEval.config.build.configFile} \
    --tree-root-file=${treefmtEval.config.projectRootFile} \
    "$@"
''
