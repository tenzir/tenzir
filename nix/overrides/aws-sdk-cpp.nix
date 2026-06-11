{
  aws-sdk-cpp,
  fetchFromGitHub,
}:

aws-sdk-cpp.overrideAttrs {
  version = "1.11.826";

  src = fetchFromGitHub {
    owner = "aws";
    repo = "aws-sdk-cpp";
    rev = "1.11.826";
    hash = "sha256-r9AM7fgtlpX+T2uZr2wao0FwcBlwPts32Bs2hn3d/tg=";
  };
}
