{pkgs}: let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv.hostPlatform) isStatic;
in
  pkgs.mkShell ({
      name = "vast-dev";
      hardeningDisable = ["fortify"] ++ lib.optional isStatic "pic";
      inputsFrom = [pkgs.vast pkgs.vast-ui];
      nativeBuildInputs =
        [pkgs.ccache pkgs.speeve pkgs.clang-tools]
        ++ pkgs.vast-integration-test-deps
        ++ lib.optionals (!(pkgs.stdenv.hostPlatform.useLLVM or false)) [
          # Make clang available as alternative compiler when it isn't the default.
          pkgs.clang_14
          # Bintools come with a wrapped lld for faster linking.
          pkgs.llvmPackages_14.bintools
        ];
      # To build libcaf_openssl with bundled CAF.
      buildInputs = [pkgs.openssl];
    }
    // lib.optionalAttrs isStatic {
      # Signal static build mode to CMake via the environment.
      VAST_ENABLE_STATIC_EXECUTABLE = "ON";
    })
