#!/bin/sh

# This script creates 4 local git aliases and installs a pre-commit hook that
# assist with working on the VAST repository:
# - `git format-show`        checks if clang-format is happy with the current
#                            staging area.
# - `git format`             formats the lines that are modified in the current
#                            staging area.
# - `git format-show-branch` checks if clang-format is happy with the changes
#                            since the current branch was created/rebased.
# - `git format-branch`      formats the code that was modified since the
#                            current branch was created/rebased.
# The pre-commit hook runs `git format-show` to check for style violations
# and prevents the commit if it finds any.

usage() {
  echo "usage: $(basename $0) [options] [<format-show|format|pre-commit>]"
  echo
  echo 'available options:'
  echo "    -f              force installation (overwrite existing)"
  echo "    -h|-?           display this help"
}

install_format_show_branch() {
  git config --local --get alias.format-show-branch >/dev/null
  if [ $? -ne 0 ] || [ "$force" = TRUE ]; then
    echo 'Adding alias for `git format-show-branch`'
    git config --local alias.format-show-branch '! git diff -U0 --no-color --cached $(git merge-base origin/master HEAD) | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -p1'
  else
    echo 'alias for `git format-show-branch` already exists, skipping'
  fi
}

install_format_show() {
  git config --local --get alias.format-show >/dev/null
  if [ $? -ne 0 ] || [ "$force" = TRUE ]; then
    echo 'Adding alias for `git format-show`'
    git config --local alias.format-show '! git diff -U0 --no-color --cached | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -p1'
  else
    echo 'alias for `git format-show` already exists, skipping'
  fi
}

install_format_branch() {
  git config --local --get alias.format-branch >/dev/null
  if [ $? -ne 0 ] || [ "$force" = TRUE ]; then
    echo 'adding alias for `git format-branch`'
    git config --local alias.format-branch '! git diff -U0 --no-color --cached $(git merge-base origin/master HEAD) | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -i -p1'
  else
    echo 'alias for `git format-branch` already exists, skipping'
  fi
}

install_format() {
  git config --local --get alias.format >/dev/null
  if [ $? -ne 0 ] || [ "$force" = TRUE ]; then
    echo 'adding alias for `git format`'
    git config --local alias.format '! git diff -U0 --no-color --cached | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -i -p1'
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
  install_format_show_branch
  install_format_branch
  install_pre_commit
else
  case "$1" in
    format-show)
      install_format_show
      ;;
    format)
      install_format
      ;;
    format-show-branch)
      install_format_show_branch
      ;;
    format-branch)
      install_format_branch
      ;;
    pre-commit)
      install_pre_commit
      ;;
  esac
fi
