{
  lib,
  stdenv,
  aws-sdk-cpp,
}:
(aws-sdk-cpp.overrideAttrs (previousAttrs: {
  cmakeFlags =
    previousAttrs.cmakeFlags
    ++ lib.optionals stdenv.hostPlatform.isDarwin [
      "-DENABLE_TESTING=OFF"
    ];
})).override
  {
    apis = [
      # arrow-cpp apis; must be kept in sync with nixpkgs.
      "cognito-identity"
      "config"
      "identity-management"
      "s3"
      "sts"
      "transfer"
      # Additional apis used by tenzir.
      "logs"
      "securitylake"
      "sqs"
    ];
  }
