# See https://nixos.wiki/wiki/FAQ/Pinning_Nixpkgs for more information on pinning
builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixpkgs-2020-08-26";
  # Commit hash for nixpkgs as of date
  url = https://github.com/NixOS/nixpkgs/archive/dc7912706e959646d82a97477f5ec637fbef4c27.tar.gz;
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "1l31c7vg7c9k2n4ysjddgqgf13gfvf0agfpxq9ln8rxss22cddhw";
}
