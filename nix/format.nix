{
  treefmtEval,
  pkgs,
}:
pkgs.writeShellScriptBin "format" ''
  set -euo pipefail
  unset PRJ_ROOT

  treefmt=${treefmtEval.config.package}/bin/treefmt
  config_file=${treefmtEval.config.build.configFile}
  submodule_prefix=contrib/tenzir-plugins
  submodule_root="$PWD/$submodule_prefix"

  "$treefmt" \
    --config-file="$config_file" \
    --tree-root-file=${treefmtEval.config.projectRootFile} \
    "$@"

  if [ -f "$submodule_prefix/README.md" ]; then
    submodule_args=()

    for arg in "$@"; do
      case "$arg" in
        "$submodule_root")
          submodule_args+=(.)
          ;;
        "$submodule_root"/*)
          submodule_args+=("''${arg#"$submodule_root"/}")
          ;;
        "$submodule_prefix")
          submodule_args+=(.)
          ;;
        "$submodule_prefix"/*)
          submodule_args+=("''${arg#"$submodule_prefix"/}")
          ;;
      esac
    done

    if [ "$#" -eq 0 ] || [ "''${#submodule_args[@]}" -gt 0 ]; then
      "$treefmt" \
        --config-file="$config_file" \
        --working-dir="$submodule_prefix" \
        --tree-root-file=README.md \
        "''${submodule_args[@]}"
    fi
  fi
''
