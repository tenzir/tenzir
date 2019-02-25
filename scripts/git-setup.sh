#!/bin/sh

usage() {
  echo "usage: $(basename $0) [options] [<format-show|format|pre-commit>]"
  echo
  echo 'available options:'
  echo "    -f              force installation (overwrite existing)"
  echo "    -h|-?           display this help"
}

install_format_show() {
  git config --local --get alias.format-show >/dev/null
  if [ $? -ne 0 ] || [ "$force" = TRUE ]; then
    echo 'adding alias for `git format-show`'
    git config --local alias.format-show '! R=0; while read line; do R=1; echo "$line"; done < <(git diff -U0 --no-color $(git merge-base origin/master HEAD) | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -p1); exit $R'
  else
    echo 'alias for `git format-show` already exists, skipping'
  fi
}

install_format() {
  git config --local --get alias.format >/dev/null
  if [ $? -ne 0 ] || [ "$force" = TRUE ]; then
    echo 'adding alias for `git format`'
    git config --local alias.format '! git diff -U0 --no-color $(git merge-base origin/master HEAD) | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -i -p1'
  else
    echo 'alias for `git format` already exists, skipping'
  fi
}

install_pre_commit() {
  GIT_DIR=$(git rev-parse --git-dir)
  PRE_COMMIT=${GIT_DIR}/hooks/pre-commit
  if [ ! -f ${PRE_COMMIT} ] || [ "$force" = TRUE ]; then
    echo 'adding pre-commit hook for format checking'
    printf "#!/bin/sh\ngit format-show" > ${PRE_COMMIT}
    chmod +x ${PRE_COMMIT}
  else
    echo 'git pre-commit hook already exists, skipping'
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
shift $(expr $OPTIND - 1)

if [ -z "$@" ]; then
  # Add all if no arguments.
  install_format_show
  install_format
  install_pre_commit
else
  case "$1" in
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
