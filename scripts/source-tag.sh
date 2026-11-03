#!/bin/sh
# SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors

# Prints the source tag of the engine build: `src-` and the hash of the git tree
# that holds exactly its sources, the paths that `source-paths` lists below
# `engine/` and all of `deployment/`. Submodules are left out, since the Nix
# build takes those dependencies from nixpkgs instead. CI tags deployment images
# with it, the engine reports it, and the platform finds it in its history.
#
#   source-tag.sh files <engine> [<deployment>]
#     From the files below the two directories: the tracked ones in a git
#     checkout, and every one elsewhere, as in the source a Nix build unpacks.
#   source-tag.sh revision <revision>...
#     From the history of the current repository, one line per revision.
#
# Both hash the same tree for the same sources, so a build can tell which of the
# tags its history lists it was built from without having that history.

set -euf

usage() {
  echo "usage: $0 files <engine> [<deployment>] | revision <revision>..." >&2
  exit 2
}

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

paths_of() {
  sed -e '/^#/d' -e '/^$/d'
}

# Index entries for the files of `$1/$2`, named below `$3`.
entries() {
  (
    cd "$1"
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
      # Only tracked files: untracked ones are generated from them. A deleted
      # one is gone from the sources too.
      git -c core.quotePath=false ls-files --stage -- "$2" |
        awk -F '\t' '$1 !~ /^160000 / { print $2 }'
    elif [ -e "$2" ]; then
      find "$2" '(' -type f -o -type l ')' | sed 's|^\./||'
    fi | while IFS= read -r file; do
      if [ -L "$file" ]; then
        # Git stores a symbolic link as the path it points to.
        printf '120000 %s\t%s/%s\n' \
          "$(readlink "$file" | tr -d '\n' | GIT_DIR=$scratch/git git hash-object -w --stdin)" \
          "$3" "$file"
      elif [ -f "$file" ]; then
        if [ -x "$file" ]; then mode=100755; else mode=100644; fi
        printf '%s\t%s\n' "$mode" "$file" >>"$scratch/files"
      fi
    done
    # One process hashes every regular file.
    [ -s "$scratch/files" ] || return 0
    cut -f 2- "$scratch/files" |
      GIT_DIR=$scratch/git git hash-object -w --no-filters --stdin-paths |
      paste "$scratch/files" - |
      awk -F '\t' -v prefix="$3" '{ print $1 " " $3 "\t" prefix "/" $2 }'
    rm "$scratch/files"
  )
}

files() {
  [ $# -ge 1 ] && [ $# -le 2 ] || usage
  engine=$(cd "$1" && pwd)
  deployment=
  if [ $# -eq 2 ]; then deployment=$(cd "$2" && pwd); fi
  git init --quiet --bare "$scratch/git"
  {
    paths_of <"$engine/scripts/source-paths" | while IFS= read -r path; do
      entries "$engine" "$path" engine
    done
    if [ -n "$deployment" ]; then entries "$deployment" . deployment; fi
  } |
    GIT_DIR=$scratch/git GIT_INDEX_FILE=$scratch/index \
      git update-index --add --index-info
  printf 'src-%s\n' \
    "$(GIT_DIR=$scratch/git GIT_INDEX_FILE=$scratch/index git write-tree)"
}

revision() {
  [ $# -ge 1 ] || usage
  # The trees this writes go to the scratch directory, not the repository.
  objects=$(git rev-parse --path-format=absolute --git-common-dir)/objects
  mkdir "$scratch/objects"
  for revision in "$@"; do
    listed=$(git show "$revision:engine/scripts/source-paths")
    paths=$(printf '%s\n' "$listed" | paths_of | sed 's|^|engine/|')
    # shellcheck disable=SC2086 # One path per word.
    git ls-tree -r --full-tree "$revision" -- $paths deployment |
      awk '$1 != "160000"' |
      GIT_INDEX_FILE=$scratch/index git update-index --add --index-info
    printf 'src-%s\n' "$(
      GIT_INDEX_FILE=$scratch/index GIT_OBJECT_DIRECTORY=$scratch/objects \
        GIT_ALTERNATE_OBJECT_DIRECTORIES=$objects git write-tree
    )"
    rm "$scratch/index"
  done
}

[ $# -ge 1 ] || usage
command=$1
shift
case $command in
  files) files "$@" ;;
  revision) revision "$@" ;;
  *) usage ;;
esac
