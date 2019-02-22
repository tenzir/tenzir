#!/usr/bin/env bash

git config --local --get "alias.format-show" >/dev/null
if [ $? -ne 0 ]; then
  echo 'Adding alias for `git format-show`'
  git config --local "alias.format-show" '! R=0; while read line; do R=1; echo "$line"; done < <(git diff -U0 --no-color $(git merge-base origin/master HEAD) | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -p1); exit $R'
fi

git config --local --get "alias.format" >/dev/null
if [ $? -ne 0 ]; then
  echo 'Adding alias for `git format`'
  git config --local "alias.format" '! git diff -U0 --no-color $(git merge-base origin/master HEAD) | $(git rev-parse --show-toplevel)/scripts/clang-format-diff.py -i -p1'
fi

GIT_DIR=$(git rev-parse --git-dir)
PRE_COMMIT=${GIT_DIR}/hooks/pre-commit
if [ ! -f ${PRE_COMMIT} ]; then
  echo 'Adding pre-commit hook for format checking'
  printf "#!/usr/bin/env bash\ngit format-show" > ${PRE_COMMIT}
  chmod +x ${PRE_COMMIT}
fi
