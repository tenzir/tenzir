{
  lib,
  buildPackages,
  stdenv,
  fetchFromGitHub,
  rustPlatform,
  installShellFiles,
  versionCheckHook,
}:

rustPlatform.buildRustPackage (finalAttrs: {
  pname = "yara-x";
  version = "1.19.0";

  src = fetchFromGitHub {
    owner = "VirusTotal";
    repo = "yara-x";
    tag = "v${finalAttrs.version}";
    hash = "sha256-CokjFTQoFT9k/2/MuQSbfzHonW4V0F8hskhqDvpCesM=";
  };

  cargoHash = "sha256-wMh8F++16tQ0IUhacBPb4rDcydmDKZKzQf8EK/qDJXo=";

  env = {
    CARGO_PROFILE_RELEASE_LTO = "fat";
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS = "1";
  };

  cargoBuildFlags = [ "--package=yara-x-cli" ];

  nativeBuildInputs = [
    installShellFiles
    buildPackages.cargo-c
  ];

  postBuild = ''
    ${buildPackages.rust.envVars.setEnv} cargo cbuild --package=yara-x-capi ${lib.optionalString stdenv.hostPlatform.isStatic "--library-type=staticlib"} --release --frozen --prefix=${placeholder "out"} --target ${stdenv.hostPlatform.rust.rustcTarget}
  '';

  postInstall = ''
    ${buildPackages.rust.envVars.setEnv} cargo cinstall --package=yara-x-capi ${lib.optionalString stdenv.hostPlatform.isStatic "--library-type=staticlib"} --release --frozen --prefix=${placeholder "out"} --target ${stdenv.hostPlatform.rust.rustcTarget}
  ''
  + lib.optionalString (stdenv.buildPlatform.canExecute stdenv.hostPlatform) ''
    installShellCompletion --cmd yr \
      --bash <($out/bin/yr completion bash) \
      --fish <($out/bin/yr completion fish) \
      --zsh <($out/bin/yr completion zsh)
  '';

  cargoTestFlags = [ "--package=yara-x-cli" ];
  checkFlags = [ "--skip=scanner::blocks::tests::block_scanner_timeout" ];
  checkType = "debug";

  nativeCheckInputs = [ versionCheckHook ];
  doInstallCheck = true;

  meta = {
    description = "Tool to do pattern matching for malware research";
    homepage = "https://virustotal.github.io/yara-x/";
    changelog = "https://github.com/VirusTotal/yara-x/releases/tag/v${finalAttrs.version}";
    license = lib.licenses.bsd3;
    mainProgram = "yr";
  };
})
