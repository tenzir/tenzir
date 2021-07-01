#!/usr/bin/env nix-shell
#!nix-shell -i bash -p coreutils git nix nix-prefetch-github nix-prefetch-git

dir=$(dirname "$(readlink -f "$0")")
toplevel=$(git -C ${dir} rev-parse --show-toplevel)
caf_submodule=libvast/aux/caf
caf_path="${toplevel}/${caf_submodule}"
caf_rev=$(git -C ${toplevel} submodule status -- ${caf_submodule} | cut -c2- | cut -d' ' -f 1)

caf_version=$(git -C "${caf_path}" describe --tag)

nix-prefetch-github --no-fetch-submodules --rev=${caf_rev} tenzir actor-framework \
  | jq --arg version ${caf_version} '. + {$version}' \
  > ${dir}/caf/source.json
