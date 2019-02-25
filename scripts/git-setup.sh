#!/bin/sh

set -e

usage() {
  echo "usage: $(basename $0) [options] [<format-show|format|pre-commit>]"
  echo
  echo 'available options:'
  echo "    -f              force installation (overwrite existing)"
  echo "    -h|-?           display this help"
}

install_format_show() {
  git config --local --get "alias.format-show" >/dev/null
  if [ "$force" = TRUE ] || [ $? -ne 0 ]; then
    echo 'Adding alias for `git format-show`'
    git config --local "alias.format-show" '! R=0; while read line; do R=1; echo "$line"; done < <(git diff -U0 --no-color $(git merge-base origin/master HEAD) | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -p1); exit $R'
  else
    echo 'Alias for `git format-show` already exists, skipping.'
  fi
}

install_format() {
  git config --local --get "alias.format" >/dev/null
  if [ "$force" = TRUE ] || [ $? -ne 0 ]; then
    echo 'Adding alias for `git format`'
    git config --local "alias.format" '! git diff -U0 --no-color $(git merge-base origin/master HEAD) | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -i -p1'
  else
    echo 'Alias for `git format` already exists, skipping.'
  fi
}

install_pre_commit() {
  GIT_DIR=$(git rev-parse --git-dir)
  PRE_COMMIT=${GIT_DIR}/hooks/pre-commit
  if [ "$force" = TRUE ] || [ ! -f ${PRE_COMMIT} ]; then
    echo 'Adding pre-commit hook for format checking'
    printf "#!/usr/bin/env bash\ngit format-show" > ${PRE_COMMIT}
    chmod +x ${PRE_COMMIT}
  else
    echo 'Git pre-commit hook already exists, skipping.'
  fi
}

force=FALSE
while getopts "fh?" opt; do
  case "$opt" in
    f)
      force=TRUE
      ;;
    h|\?)
      usage
      exit 0
    ;;
  esac
done

if [ "$#" -lt "$OPTIND" ]; then
  # Add all if no arguments.
  install_format_show
  install_format
  install_pre_commit
else
  shift $(expr $OPTIND - 1)
  part=$1
  case "$part" in
    format-show)
      install_format_show
      ;;
    format)
      install_format
      ;;
    pre-commit)
      install_pre_commit
      ;;
  esac
fi
