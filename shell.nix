{ pkgs ? import <nixpkgs> { }, useClang ? false }:
let
  inherit (pkgs) lib;
  llvmPkgs = pkgs.llvmPackages_10;
  stdenv = if useClang then llvmPkgs.libcxxStdenv else pkgs.stdenv;
  mkShell = pkgs.mkShell.override { inherit stdenv; };
  vast =
    ((import ./default.nix { inherit pkgs; }).override{
      versionOverride = "dev";
    }).overrideAttrs (old: {
      src = null;
    });
in
mkShell {
  name = "vast-dev-" + (if useClang then "clang" else "gcc");
  hardeningDisable = [ "fortify" ];
  inputsFrom = [ vast ];
  buildInputs = [ llvmPkgs.lldClang.bintools ];
  NIX_CFLAGS_LINK = "-fuse-ld=lld";
}
