#!/usr/bin/env nix-shell
#!nix-shell -i bash -p coreutils git nix nix-prefetch-github nix-prefetch-git

dir=$(dirname "$(readlink -f "$0")")
toplevel=$(git -C ${dir} rev-parse --show-toplevel)
caf_submodule=aux/caf
caf_path="${toplevel}/${caf_submodule}"
caf_rev=$(git -C ${toplevel} submodule status -- ${caf_submodule} | cut -c2- | cut -d' ' -f 1)
broker_submodule=aux/broker/broker
broker_path="${toplevel}/${broker_submodule}"
broker_rev=$(git -C ${toplevel} submodule status -- ${broker_submodule} | cut -c2- | cut -d' ' -f 1)

caf_version=$(git -C "${caf_path}" describe --tag)
broker_version=$(git -C "${broker_path}" describe --tag)

nix-prefetch-github --rev=${caf_rev} actor-framework actor-framework \
  | jq --arg version ${caf_version} '. + {$version}' \
  > ${dir}/caf/source.json
nix-prefetch-git --quiet --rev ${broker_rev} --fetch-submodules https://github.com/zeek/broker \
  | jq --arg version ${broker_version} '. + {$version}' \
  > ${dir}/broker/source.json
