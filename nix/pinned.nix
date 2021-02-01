# See https://nixos.wiki/wiki/FAQ/Pinning_Nixpkgs for more information on pinning
builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixpkgs-2021-02-01";
  # Commit hash for nixpkgs as of date
  url = https://github.com/NixOS/nixpkgs/archive/449b698a0b554996ac099b4e3534514528019269.tar.gz;
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "0skzcxsd45impdw3w5l7764albsx5h2z93l2d384dcn0ckxsacy7";
}
