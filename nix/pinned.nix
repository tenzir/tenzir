# See https://nixos.wiki/wiki/FAQ/Pinning_Nixpkgs for more information on pinning
builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixpkgs-2021-06-29";
  # Commit hash for nixpkgs as of date
  url = https://github.com/NixOS/nixpkgs/archive/db6e089456cdddcd7e2c1d8dac37a505c797e8fa.tar.gz;
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "02yk20i9n5nhn6zgll3af7kp3q5flgrpg1h5vcqfdqcck8iikx4b";
}
