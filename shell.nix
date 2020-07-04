{ pkgs ? import ./nix { }, useClang ? false }:
let
  inherit (pkgs) lib;
  llvmPkgs = pkgs.buildPackages.llvmPackages_10;
  stdenv = if useClang then llvmPkgs.libcxxStdenv else pkgs.stdenv;
  static_stdenv = stdenv.hostPlatform.isMusl;
  mkShell = pkgs.mkShell.override { inherit stdenv; };
in
mkShell ({
  name = "vast-dev-" + (if useClang then "clang" else "gcc");
  hardeningDisable = [ "fortify" ] ++ lib.optional static_stdenv "pic";
  inputsFrom = [ pkgs.vast ];
  shellHook = ''
    echo "Entering Event Horizon environment"
  '';
} // lib.optionalAttrs static_stdenv {
  VAST_STATIC_EXECUTABLE = "ON";
  ZSTD_ROOT = "${pkgs.zstd}";
} // lib.optionalAttrs (stdenv.isLinux && !static_stdenv) {
  nativeBuildInputs = [ llvmPkgs.lldClang.bintools ];
  NIX_CFLAGS_LINK = "-fuse-ld=lld";
})
