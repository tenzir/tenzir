{ lib,
  stdenvNoCC,
  fetchurl,
}:
let
  version = "0.2.17";
  base_url = "https://github.com/astral-sh/uv/releases/download";

  sources = {
    aarch64-darwin = {
      stem = "uv-aarch64-apple-darwin";
      sha256 = "7dc5fe97867ac3bbcbeabf32fb76b3caaf12141b5e20926ed81353f9a2ff7dcb";
    };
    x86_86-darwin = {
      stem = "uv-x86_64-apple-darwin";
      sha256 = "78137a1b9d6fd1f8f5f0d4208829dd5f89b6505c314192c0fa2a35d6faff5e91";
    };
    aarch64-linux = {
      stem = "uv-aarch64-unknown-linux-musl";
      sha256 = "910edd0a8db6ff39baaad0f7f77b2ce74e0111b6b83a4439e2e02d5b82404e1d";
    };
    x86_64-linux = {
      stem = "uv-x86_64-unknown-linux-musl";
      sha256 = "20184a870ba25416b61d46c387853afd27d9a8df3f0598ee6878a315db5c7302";
    };
  };

  inherit (stdenvNoCC.hostPlatform) system;
  source = sources."${system}";
in
stdenvNoCC.mkDerivation {
  pname = "uv-binary";
  inherit version;

  src = fetchurl {
    url = "${base_url}/${version}/${source.stem}.tar.gz";
    inherit (source) sha256;
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
