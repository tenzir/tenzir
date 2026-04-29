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

  path_count=0
  submodule_path_count=0
  submodule_args=()

  add_path_arg() {
    local arg="$1"

    path_count=$((path_count + 1))
    case "$arg" in
      "$submodule_root"|"$submodule_root"/)
        submodule_args+=(.)
        submodule_path_count=$((submodule_path_count + 1))
        ;;
      "$submodule_root"/*)
        submodule_args+=("''${arg#"$submodule_root"/}")
        submodule_path_count=$((submodule_path_count + 1))
        ;;
      "$submodule_prefix"|"$submodule_prefix"/|"./$submodule_prefix"|"./$submodule_prefix"/)
        submodule_args+=(.)
        submodule_path_count=$((submodule_path_count + 1))
        ;;
      "$submodule_prefix"/*)
        submodule_args+=("''${arg#"$submodule_prefix"/}")
        submodule_path_count=$((submodule_path_count + 1))
        ;;
      "./$submodule_prefix"/*)
        submodule_args+=("''${arg#"./$submodule_prefix"/}")
        submodule_path_count=$((submodule_path_count + 1))
        ;;
      *)
        submodule_args+=("$arg")
        ;;
    esac
  }

  consume_option_value=false
  paths_only=false
  for arg in "$@"; do
    if $consume_option_value; then
      submodule_args+=("$arg")
      consume_option_value=false
      continue
    fi

    if $paths_only; then
      add_path_arg "$arg"
      continue
    fi

    case "$arg" in
      --)
        submodule_args+=("$arg")
        paths_only=true
        ;;
      --completion|--config-file|--cpu-profile|--excludes|--formatters|--on-unmatched|--tree-root|--tree-root-cmd|--tree-root-file|--walk|--working-dir)
        submodule_args+=("$arg")
        consume_option_value=true
        ;;
      --completion=*|--config-file=*|--cpu-profile=*|--excludes=*|--formatters=*|--on-unmatched=*|--tree-root=*|--tree-root-cmd=*|--tree-root-file=*|--walk=*|--working-dir=*)
        submodule_args+=("$arg")
        ;;
      -f|-u|-C)
        submodule_args+=("$arg")
        consume_option_value=true
        ;;
      -f?*|-u?*|-C?*)
        submodule_args+=("$arg")
        ;;
      -*)
        submodule_args+=("$arg")
        ;;
      *)
        add_path_arg "$arg"
        ;;
    esac
  done

  if [ "$path_count" -gt 0 ]; then
    if [ "$submodule_path_count" -eq "$path_count" ]; then
      if [ ! -f "$submodule_prefix/README.md" ]; then
        echo "error: cannot format $submodule_prefix because the submodule is not checked out" >&2
        exit 1
      fi
      "$treefmt" \
        --config-file="$config_file" \
        --working-dir="$submodule_prefix" \
        --tree-root-file=README.md \
        "''${submodule_args[@]}"
    elif [ "$submodule_path_count" -gt 0 ]; then
      echo "error: cannot format main repository and $submodule_prefix paths in one invocation" >&2
      exit 1
    else
      "$treefmt" \
        --config-file="$config_file" \
        --tree-root-file=${treefmtEval.config.projectRootFile} \
        "$@"
    fi
  else
    "$treefmt" \
      --config-file="$config_file" \
      --tree-root-file=${treefmtEval.config.projectRootFile} \
      "$@"

    if [ -f "$submodule_prefix/README.md" ]; then
      "$treefmt" \
        --config-file="$config_file" \
        --working-dir="$submodule_prefix" \
        --tree-root-file=README.md \
        "$@"
    fi
  fi
''
