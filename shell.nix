{ pkgs }:
let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv.hostPlatform) isStatic;
in
  pkgs.mkShell.override {stdenv = pkgs.gcc13Stdenv;} ({
      name = "tenzir-dev";
      hardeningDisable = ["fortify"] ++ lib.optional isStatic "pic";
      inputsFrom = [pkgs.tenzir-de pkgs.tenzir-de.unchecked];
      nativeBuildInputs =
        [
          pkgs.ccache
          pkgs.clang-tools_16
          pkgs.cmake-format
          pkgs.speeve
          pkgs.shfmt
          pkgs.poetry
          pkgs.python3Packages.spdx-tools
          pkgs.uv
          (pkgs.python3.withPackages (ps: with ps; [
            aiohttp
            dynaconf
            numpy
            pandas
            pyarrow
            python-box
          ]))
        ] ++ pkgs.tenzir-integration-test-deps
          ++ lib.optionals (!(pkgs.stdenv.hostPlatform.useLLVM or false)) [
          # Make clang available as alternative compiler when it isn't the default.
          pkgs.clang_16
          # Bintools come with a wrapped lld for faster linking.
          pkgs.llvmPackages_16.bintools
        ] # Temporarily only on Linux.
          ++ lib.optionals pkgs.stdenv.isLinux [
          pkgs.pandoc
        ];
      # To build libcaf_openssl with bundled CAF.
      buildInputs = [pkgs.openssl];
      shellHook = ''
        # Use editable mode for python code part of the python operator. This
        # makes changes to the python code observable in the python operator
        # without needing to rebuild the wheel.
        export TENZIR_PLUGINS__PYTHON__IMPLICIT_REQUIREMENTS="-e $PWD/python"
        # uv is provided in the nativeBuildInputs above.
        export TENZIR_ENABLE_BUNDLED_UV=OFF
        export PYTHONPATH="$PYTHONPATH''${PYTHONPATH:+:}$PWD/python"
      '';
    }
    // lib.optionalAttrs isStatic {
      # Signal static build mode to CMake via the environment.
      env.TENZIR_ENABLE_STATIC_EXECUTABLE = "ON";
    })
