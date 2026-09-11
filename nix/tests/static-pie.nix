{
  lib,
  stdenv,
  buildPackages,
  writeText,
}:
let
  source = writeText "static-pie.cpp" ''
    #include <iostream>

    auto main() -> int {
      std::cout << "static-pie works\n";
    }
  '';
in
stdenv.mkDerivation {
  name = "check-static-pie";
  dontUnpack = true;
  nativeBuildInputs = [ buildPackages.binutils ];
  # Exercise the compiler defaults without the wrapper's PIC hardening flag.
  hardeningDisable = [ "pic" ];

  buildPhase = ''
    runHook preBuild
    $CXX -static ${source} -o pie
    $CXX -static -no-pie ${source} -o no-pie
    $READELF -h pie | grep -E 'Type:.*DYN'
    $READELF -h no-pie | grep -E 'Type:.*EXEC'
    for binary in pie no-pie; do
      if $READELF -l "$binary" | grep -q INTERP; then
        echo "$binary unexpectedly has a dynamic interpreter" >&2
        exit 1
      fi
      if $READELF -d "$binary" | grep -q NEEDED; then
        echo "$binary unexpectedly needs shared libraries" >&2
        exit 1
      fi
    done
    $CXX -c ${source} -o probe.o
    $CXX -static -nostdlib -r probe.o -o relocatable.o
    $READELF -h relocatable.o | grep -E 'Type:.*REL'
    ${lib.optionalString (stdenv.buildPlatform.canExecute stdenv.hostPlatform) ''
      test "$(./pie)" = 'static-pie works'
      test "$(./no-pie)" = 'static-pie works'
    ''}
    runHook postBuild
  '';

  installPhase = ''
    mkdir -p "$out/bin"
    cp pie no-pie "$out/bin/"
  '';
}
