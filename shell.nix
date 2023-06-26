{pkgs}: let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv.hostPlatform) isStatic;
in
  pkgs.mkShell ({
      name = "tenzir-dev";
      hardeningDisable = ["fortify"] ++ lib.optional isStatic "pic";
      inputsFrom = [pkgs.tenzir];
      nativeBuildInputs =
        [pkgs.ccache pkgs.speeve pkgs.clang-tools]
        ++ pkgs.tenzir-integration-test-deps
        ++ lib.optionals (!(pkgs.stdenv.hostPlatform.useLLVM or false)) [
          # Make clang available as alternative compiler when it isn't the default.
          pkgs.clang_16
          # Bintools come with a wrapped lld for faster linking.
          pkgs.llvmPackages_16.bintools
        ];
      # To build libcaf_openssl with bundled CAF.
      buildInputs = [pkgs.openssl];
    }
    // lib.optionalAttrs isStatic {
      # Signal static build mode to CMake via the environment.
      VAST_ENABLE_STATIC_EXECUTABLE = "ON";
    })
