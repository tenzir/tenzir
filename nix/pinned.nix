# See https://nixos.wiki/wiki/FAQ/Pinning_Nixpkgs for more information on pinning
builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixpkgs-unstable-2020-06-07";
  # Commit hash for nixpkgs-unstable as of date
  url = https://github.com/NixOS/nixpkgs-channels/archive/029a5de08390bb03c3f44230b064fd1850c6658a.tar.gz;
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "03fjkzhrs2avcvdabgm7a65rnyjaqbqdnv4q86qyjkkwg64g5m8x";
}
