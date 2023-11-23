{pkgs}: let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv.hostPlatform) isStatic;
in
  pkgs.mkShell.override {stdenv = pkgs.gcc13Stdenv;} ({
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
        # Use editable mode for python code part of the python operator.
        # This makes changes to the pytenzir code observable in the operator
        # without needing to rebuild the wheel.
        export TENZIR_PLUGINS__PYTHON__IMPLICIT_REQUIREMENTS="-e $PWD/python"
      '';
    }
    // lib.optionalAttrs isStatic {
      # Signal static build mode to CMake via the environment.
      env.TENZIR_ENABLE_STATIC_EXECUTABLE = "ON";
    })
