# See https://nixos.wiki/wiki/FAQ/Pinning_Nixpkgs for more information on pinning
builtins.fetchTarball {
  # Descriptive name to make the store path easier to identify
  name = "nixpkgs-2020-11-18";
  # Commit hash for nixpkgs as of date
  url = https://github.com/NixOS/nixpkgs/archive/30d7b9341291dbe1e3361a5cca9052ee1437bc97.tar.gz;
  # Hash obtained using `nix-prefetch-url --unpack <url>`
  sha256 = "0xvkspx2airqj581a233j16wrhw9dgi14b7sj7hjjql91s0r8gpa";
}
