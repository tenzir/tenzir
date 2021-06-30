{ nixpkgs ? import ./nix/pinned.nix
, pkgs ? import ./nix { nixpkgs = nixpkgs; }
, useClang ? pkgs.stdenv.isDarwin
}:
let
  inherit (pkgs) lib;
  llvmPkgs = pkgs.buildPackages.llvmPackages_10;
  stdenv = if useClang then llvmPkgs.stdenv else pkgs.stdenv;
  inherit (stdenv.hostPlatform) isStatic;
  mkShell = pkgs.mkShell.override { inherit stdenv; };
in
mkShell ({
  name = "vast-dev-" + (if useClang then "clang" else "gcc");
  hardeningDisable = [ "fortify" ] ++ lib.optional isStatic "pic";
  inputsFrom = [ pkgs.vast ];
  # To build libcaf_openssl with bundled CAF.
  buildInputs = [ pkgs.openssl ];
  shellHook = ''
    echo "Entering VAST environment"
  '';
} // lib.optionalAttrs isStatic {
  VAST_STATIC_EXECUTABLE = "ON";
} // lib.optionalAttrs (stdenv.isLinux && !isStatic) {
  nativeBuildInputs = [ llvmPkgs.bintools ];
})
