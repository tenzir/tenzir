#!/usr/bin/env bash

set -eu

is_shell() {
    [[ $1 == *.sh ]] && shell="sh" && return 0
    [[ $1 == *.bash ]] && shell="bash" && return 0
    [[ $(file -b --mime-type "$1") != text/x-shellscript ]] && return 1
    # shellcheck disable=SC2155
    local ft=$(file -b "$1")
    case ${ft%%,*} in
      "POSIX shell script")
        shell="sh"
        return 0
        ;;
      "Bourne-Again shell script")
        shell="bash"
        return 0
        ;;
      *)
        return 1
        ;;
    esac
}

while read -r file; do
    if is_shell "$file"; then
        >&2 echo "Checking $file as $shell..."
        shellcheck --norc -W0 -s "$shell" "$file" || continue
    fi
done
