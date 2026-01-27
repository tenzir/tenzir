{
  lib,
  stdenv,
  aws-sdk-cpp,
}:
(aws-sdk-cpp.overrideAttrs (previousAttrs: {
  cmakeFlags =
    previousAttrs.cmakeFlags ++ [
       "-DDISABLE_INTERNAL_IMDSV1_CALLS=ON"
    ]
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
      "sqs"
    ];
  }
