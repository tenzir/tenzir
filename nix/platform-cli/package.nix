{
  lib,
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
  nodeModules = stdenvNoCC.mkDerivation {
    pname = "platform-cli-node-modules";
    version = "unstable";
    src = source;

    dontConfigure = true;
    dontFixup = true;

    buildPhase = ''
      runHook preBuild

      export HOME="$TMPDIR"
      export BUN_INSTALL_CACHE_DIR="$TMPDIR/bun-cache"
      ${compiler}/bin/bun install \
        --frozen-lockfile \
        --production \
        --ignore-scripts \
        --no-progress

      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall

      mkdir -p "$out"
      cp -R node_modules "$out/"

      runHook postInstall
    '';

    outputHash = "sha256-PCuGxkLUxXz8HqgQEGHMmFlq4P7CTd56oY4RIDFX+Zc=";
    outputHashAlgo = "sha256";
    outputHashMode = "recursive";
  };
in
stdenvNoCC.mkDerivation {
  pname = "platform-cli";
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
      --outfile platform-cli

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    install -Dm755 platform-cli "$out/bin/platform-cli"

    runHook postInstall
  '';

  doInstallCheck = true;
  installCheckPhase = ''
    runHook preInstallCheck

    ${lib.optionalString stdenvNoCC.hostPlatform.isLinux ''
      ${file}/bin/file "$out/bin/platform-cli" | grep -q "statically linked"
      ! ${binutils}/bin/readelf -l "$out/bin/platform-cli" | grep -q INTERP
      ! ${binutils}/bin/readelf -d "$out/bin/platform-cli" | grep -q NEEDED
    ''}
    ${lib.optionalString stdenvNoCC.hostPlatform.isDarwin ''
      ${file}/bin/file "$out/bin/platform-cli" | grep -q "Mach-O.*arm64"
      ! ${cctools}/bin/otool -L "$out/bin/platform-cli" | grep -q /nix/store
    ''}
    "$out/bin/platform-cli" --help >/dev/null

    runHook postInstallCheck
  '';

  meta = {
    description = "Command-line client for the Tenzir Platform";
    mainProgram = "platform-cli";
    platforms = lib.platforms.linux ++ [ "aarch64-darwin" ];
  };
}
