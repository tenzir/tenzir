{
  buildGoModule,
  fetchFromGitHub,
}:
buildGoModule rec {
  pname = "speeve";
  version = "0.1.3";
  vendorHash = "sha256-Mw1cRIwmDS2Canljkuw96q2+e+z14MUcU5EtupUcTDQ=";
  src = fetchFromGitHub {
    rev = "v${version}";
    owner = "satta";
    repo = pname;
    hash = "sha256-75QrtuOduUNT9g2RJRWUow8ESBqsDDXCMGVNQKFc+SE=";
  };
  # upstream does not provide a go.sum file
  preBuild = ''
    cp ${./go.sum} go.sum
  '';
  subPackages = [ "cmd/speeve" ];
}
