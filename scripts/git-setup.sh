#!/bin/sh
# shellcheck disable=SC2016

# This script creates 4 local git aliases and installs a pre-push hook that
# assists with working in the Tenzir repository:
# - `git format-show`        checks if clang-format is happy with the current
#                            staging area.
# - `git format`             formats the lines that are modified in the current
#                            staging area.
# - `git format-show-branch` checks if clang-format is happy with the changes
#                            since the current branch was created/rebased.
# - `git format-branch`      formats the code that was modified since the
#                            current branch was created/rebased.
# The pre-push hook runs `git format-show-branch` to check for style violations
# and prevents the push if it finds any.

usage() {
  echo "usage: $(basename "$0") [options] [<format-show|format|pre-push>]"
  echo
  echo 'available options:'
  echo "    -f              force installation (overwrite existing)"
  echo "    -p <path>       path to clang-format-diff.py"
  echo "    -h|-?           display this help"
}

install_format_show_branch() {
  if ! git config --local --get alias.format-show-branch >/dev/null || [ "$force" = TRUE ]; then
    echo 'Adding alias for `git format-show-branch`'
    git config --local alias.format-show-branch "!f() { git diff -U0 --no-color \$@ \$(git merge-base origin/main HEAD) HEAD -- \"*.cpp\" \"*.cpp.in\" \"*.hpp\" \"*.hpp.in\" | ${format_call} -p1; }; f"
  else
    echo 'alias for `git format-show-branch` already exists, skipping'
  fi
}

install_format_show() {
  if ! git config --local --get alias.format-show >/dev/null || [ "$force" = TRUE ]; then
    echo 'Adding alias for `git format-show`'
    git config --local alias.format-show "!f() { git diff -U0 --no-color \$@ -- \"*.cpp\" \"*.hpp\" | ${format_call} -p1; }; f"
  else
    echo 'alias for `git format-show` already exists, skipping'
  fi
}

install_format_branch() {
  if ! git config --local --get alias.format-branch >/dev/null || [ "$force" = TRUE ]; then
    echo 'adding alias for `git format-branch`'
    git config --local alias.format-branch "!f() { git diff -U0 --no-color \$@ \$(git merge-base origin/main HEAD) -- \"*.cpp\" \"*.hpp\" | ${format_call} -i -p1; }; f"
  else
    echo 'alias for `git format-branch` already exists, skipping'
  fi
}

install_format() {
  if ! git config --local --get alias.format >/dev/null || [ "$force" = TRUE ]; then
    echo 'adding alias for `git format`'
    git config --local alias.format "!f() { git diff -U0 --no-color \$@ -- \"*.cpp\" \"*.hpp\" | ${format_call} -i -p1; }; f"
  else
    echo 'alias for `git format` already exists, skipping'
  fi
}

install_pre_push() {
  GIT_DIR="$(git rev-parse --git-common-dir)"
  PRE_PUSH="${GIT_DIR}/hooks/pre-push"
  PRE_COMMIT="${GIT_DIR}/hooks/pre-commit"
  # Migrate away from the legacy pre-commit format hook.
  if [ -f "${PRE_COMMIT}" ] && grep -q 'git format-show --cached' "${PRE_COMMIT}"; then
    echo 'removing legacy pre-commit format hook'
    rm "${PRE_COMMIT}"
  fi
  if [ ! -f "${PRE_PUSH}" ] || [ "$force" = TRUE ]; then
    echo 'adding pre-push hook for format checking'
    printf "#!/bin/sh\ngit format-show-branch" >"${PRE_PUSH}"
    chmod +x "${PRE_PUSH}"
  else
    echo 'git pre-push hook already exists, skipping'
  fi
}

dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
current_git_tl="$(git rev-parse --show-toplevel)"
script_git_tl="$(git -C "${dir}" rev-parse --show-toplevel)"
force=FALSE
fixed_path=FALSE
clang_format_diff="${dir}/clang-format-diff.py"
format_call='$(git rev-parse --show-toplevel)/scripts/clang-format-diff.py'
while getopts "fp:h?" opt; do
  case "$opt" in
    f)
      force=TRUE
      ;;
    p)
      fixed_path=TRUE
      clang_format_diff="$OPTARG"
      ;;
    h | \?)
      usage
      exit 0
      ;;
  esac
done
shift $((OPTIND - 1))

if [ ! "${script_git_tl}" = "${current_git_tl}" ]; then
  fixed_path=TRUE
fi
if [ "${fixed_path}" = TRUE ]; then
  echo "using fixed path to clang_format_diff: ${clang_format_diff}"
  format_call="${clang_format_diff}"
fi

if [ -z "$*" ]; then
  # Add all if no arguments.
  install_format_show
  install_format
  install_format_show_branch
  install_format_branch
  install_pre_push
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
    pre-push)
      install_pre_push
      ;;
  esac
fi
