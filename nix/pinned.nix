# See https://nixos.wiki/wiki/FAQ/Pinning_Nixpkgs for more information on pinning
builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixpkgs-2021-11-04";
  # Commit hash for nixpkgs as of date
  url = https://github.com/NixOS/nixpkgs/archive/4789953e5c1ef6d10e3ff437e5b7ab8eed526942.tar.gz;
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "sha256:15nksgi9pncz59l8vrfg05g9dqw835wwdi9bmghffyg0k1yh2j8d";
}
