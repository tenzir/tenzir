{ pkgs ? import <nixpkgs> { } }:

let
  project = pkgs.callPackage ./yarn-project.nix {
    # Example of selecting a specific version of Node.js.
    nodejs = pkgs.nodejs-18_x;
  } {
    # Example of providing a different source tree.
    src = pkgs.lib.cleanSource ./.;
  };

in project.overrideAttrs (oldAttrs: {
  # Example of adding packages to the build environment.
  # Especially dependencies with native modules may need a Python installation.
  # buildInputs = oldAttrs.buildInputs ++ [ pkgs.python3 ];

  # Example of invoking a build step in your project.
  buildPhase = ''
    yarn build
  '';

  installPhase = ''
    mkdir -p $out
    cp -r build $out
  '';
})
