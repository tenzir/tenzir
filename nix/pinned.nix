# See https://nixos.wiki/wiki/FAQ/Pinning_Nixpkgs for more information on pinning
builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixpkgs-2020-12-17";
  # Commit hash for nixpkgs as of date
  url = https://github.com/NixOS/nixpkgs/archive/f42d6f81a8c6fffae2b2083d1c1dca1270c82e7b.tar.gz;
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "15s7vz4r0333k2ip9v8fqgjwc3wrdfsfl9a8bjk4120glhl1ml5k";
}
