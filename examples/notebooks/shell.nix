# brings poetry into scope and makes sure that poetry-installed C-lib basedpython
# python dependencies can "see" libstdc++. Ref https://nixos.wiki/wiki/Packaging/Quirks_and_Caveats#ImportError:_libstdc.2B.2B.so.6:_cannot_open_shared_object_file:_No_such_file
{pkgs ? (import <nixpkgs> {}).pkgs}:
with pkgs;
  mkShell {
    buildInputs = [
      poetry
    ];
    shellHook = ''
      # fixes libstdc++ issues and libgl.so issues
      LD_LIBRARY_PATH=${stdenv.cc.cc.lib}/lib/:/run/opengl-driver/lib/
    '';
  }
