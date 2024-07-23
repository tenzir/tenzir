{ lib,
  stdenvNoCC,
  fetchurl,
}:
let
  inherit (stdenvNoCC.hostPlatform) system;
  sources = lib.importJSON ../../python/uv-source-info.json;
  source = sources."${system}";
in
stdenvNoCC.mkDerivation {
  pname = "uv-binary";
  inherit (sources) version;

  src = fetchurl {
    inherit (source) url sha256;
  };

  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    runHook preInstall
    install -Dm 755 -t $out/bin uv
    runHook postInstall
  '';

  meta = {
    description = "Extremely fast Python package installer and resolver";
    homepage = "https://astral.sh/";
    license = with lib.licenses; [ asl20 mit ];
    mainProgram = "uv";
    maintainers = with lib.maintainers; [tobim];
    platforms = lib.attrNames sources;
    sourceProvenance = with lib.sourceTypes; [ binaryNativeCode ];
  };
}
