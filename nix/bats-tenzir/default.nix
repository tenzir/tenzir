{
  lib,
  stdenv,
}:
stdenv.mkDerivation {
  pname = "bats-tenzir";
  version = "0.1";
  src = lib.fileset.toSource {
    root = ./../../tenzir/bats/lib/bats-tenzir;
    fileset = ./../../tenzir/bats/lib/bats-tenzir;
  };
  dontBuild = true;
  installPhase = ''
    mkdir -p "$out/share/bats/bats-tenzir"
    cp load.bash "$out/share/bats/bats-tenzir"
    cp -r src "$out/share/bats/bats-tenzir"
  '';
  meta = {
    platforms = lib.platforms.all;
    license = lib.licenses.bsd3;
    #maintainers = [ ];
  };
}
