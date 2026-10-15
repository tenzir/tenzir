{
  lib,
  callPackage,
  stdenvNoCC,
  pkgsStatic,
  bun,
  unzip,
  cctools,
  file,
  binutils,
  src,
}:

let
  compiler = if stdenvNoCC.hostPlatform.isLinux then pkgsStatic.bun-unwrapped else bun;
  source = lib.fileset.toSource {
    root = src;
    fileset = lib.fileset.unions [
      (src + "/bun.lock")
      (src + "/package.json")
      (src + "/patches")
      (src + "/services/shared")
      (src + "/tools/platform-cli/main.ts")
    ];
  };
  nodeModules = callPackage ./bun-node-modules.nix { } {
    pname = "tenzir-platform-node-modules";
    src = source;
  };
in
stdenvNoCC.mkDerivation {
  pname = "tenzir-platform";
  version = "unstable";
  src = source;

  dontConfigure = true;
  nativeBuildInputs = lib.optional stdenvNoCC.hostPlatform.isDarwin unzip;

  buildPhase = ''
    runHook preBuild

    cp -R ${nodeModules}/node_modules .
    ${lib.optionalString stdenvNoCC.hostPlatform.isDarwin ''
      # Embed the upstream runtime: Nix's Bun references a Nix-store ICU dylib.
      unzip ${bun.src} -d bun-runtime
    ''}
    ${compiler}/bin/bun build --compile \
      ${lib.optionalString stdenvNoCC.hostPlatform.isDarwin "--compile-executable-path=bun-runtime/bun-darwin-aarch64/bun"} \
      tools/platform-cli/main.ts \
      --outfile tenzir-platform

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    install -Dm755 tenzir-platform "$out/bin/tenzir-platform"

    runHook postInstall
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck

    ${lib.optionalString stdenvNoCC.hostPlatform.isLinux ''
      ${file}/bin/file "$out/bin/tenzir-platform" | grep -q "statically linked"
      ! ${binutils}/bin/readelf -l "$out/bin/tenzir-platform" | grep -q INTERP
      ! ${binutils}/bin/readelf -d "$out/bin/tenzir-platform" | grep -q NEEDED
    ''}
    ${lib.optionalString stdenvNoCC.hostPlatform.isDarwin ''
      ${file}/bin/file "$out/bin/tenzir-platform" | grep -q "Mach-O.*arm64"
      ! ${cctools}/bin/otool -L "$out/bin/tenzir-platform" | grep -q /nix/store
    ''}
    "$out/bin/tenzir-platform" --help >/dev/null

    runHook postInstallCheck
  '';

  meta = {
    description = "Command-line client for the Tenzir Platform";
    mainProgram = "tenzir-platform";
    platforms = lib.platforms.linux ++ [ "aarch64-darwin" ];
  };
}
