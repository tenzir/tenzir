{
  pkgs,
  package,
}:
let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv.hostPlatform) isStatic;
in
pkgs.mkShell (
  {
    name = "tenzir-dev";
    hardeningDisable = [ "fortify" ] ++ lib.optional isStatic "pic";
    inputsFrom = [
      package.tenzir-de
      package.tenzir-de.unchecked
    ];
    nativeBuildInputs =
      [
        pkgs.ccache
        pkgs.clang-tools
        pkgs.cmake-format
        pkgs.nixfmt-rfc-style
        pkgs.speeve
        pkgs.shfmt
        pkgs.poetry
        pkgs.python3Packages.spdx-tools
        pkgs.ruff
        pkgs.uv
        (pkgs.python3.withPackages (
          ps: with ps; [
            aiohttp
            dynaconf
            numpy
            pandas
            pyarrow
            python-box
          ]
        ))
      ]
      ++ package.tenzir-integration-test-deps
      ++ lib.optionals (!(pkgs.stdenv.hostPlatform.useLLVM or false)) [
        # Make clang available as alternative compiler when it isn't the default.
        pkgs.clang
        # Bintools come with a wrapped lld for faster linking.
        pkgs.llvmPackages.bintools
      ]
      ++ lib.optionals pkgs.stdenv.isLinux [
        # Temporarily only on Linux.
        pkgs.pandoc
        pkgs.gdb
      ];
    # To build libcaf_openssl with bundled CAF.
    buildInputs = [
      pkgs.openssl
      pkgs.arrow-adbc-go
    ];
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
  }
)
