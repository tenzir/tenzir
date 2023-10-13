{pkgs}: let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv.hostPlatform) isStatic;
in
  pkgs.mkShell ({
      name = "tenzir-dev";
      hardeningDisable = ["fortify"] ++ lib.optional isStatic "pic";
      inputsFrom = [pkgs.tenzir-de];
      nativeBuildInputs =
        [
          pkgs.ccache
          pkgs.speeve
          pkgs.clang-tools_16
        ] ++ pkgs.tenzir-functional-test-deps
          ++ pkgs.tenzir-integration-test-deps
          ++ lib.optionals (!(pkgs.stdenv.hostPlatform.useLLVM or false)) [
          # Make clang available as alternative compiler when it isn't the default.
          pkgs.clang_16
          # Bintools come with a wrapped lld for faster linking.
          pkgs.llvmPackages_16.bintools
          pkgs.cmake-format
          pkgs.pandoc
          pkgs.poetry
          pkgs.python3Packages.spdx-tools
        ];
      # To build libcaf_openssl with bundled CAF.
      buildInputs = [pkgs.openssl];
      shellHook = ''
        # Prepend pytenzir to the PYTHONPATH.
        export PYTHONPATH=$PWD/python''${PYTHONPATH:+:}$PYTHONPATH
      '';
    }
    // lib.optionalAttrs isStatic {
      # Signal static build mode to CMake via the environment.
      env.TENZIR_ENABLE_STATIC_EXECUTABLE = "ON";
    })
