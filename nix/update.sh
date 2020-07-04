#!/usr/bin/env nix-shell
#!nix-shell -i bash -p coreutils git nix nix-prefetch-github nix-prefetch-git

dir=$(dirname "$(readlink -f "$0")")
toplevel=$(git -C ${dir} rev-parse --show-toplevel)
caf_rev=$(git -C ${toplevel} submodule status -- aux/caf | cut -c2- | cut -d' ' -f 1)
broker_rev=$(git -C ${toplevel} submodule status -- aux/broker/broker | cut -c2- | cut -d' ' -f 1)

nix-prefetch-github --rev=${caf_rev} actor-framework actor-framework \
  > ${dir}/caf/source.json
nix-prefetch-git --quiet --rev ${broker_rev} --fetch-submodules https://github.com/zeek/broker \
  > ${dir}/broker/source.json
