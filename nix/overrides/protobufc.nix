{
  lib,
  protobuf,
  fetchpatch2,
  protobufc,
  buildPackages,
  stdenv,
}:
(protobufc.override {
  protobuf_33 = protobuf;
}).overrideAttrs
  (base: {
    patches = (base.patches or [ ]) ++ [
      (fetchpatch2 {
        name = "protobuf-c-protobuf-34-compat.patch";
        url = "https://github.com/protobuf-c/protobuf-c/commit/d39f001b4578966600de0aaf7fc665eec6e057e5.patch?full_index=1";
        hash = "sha256-VMOmvpBVZFFkCgKCuJObnJp/tF1VaYls7nFugWp/YvI=";
      })
    ];
    env =
      let
        baseEnv = base.env or { };
      in
      baseEnv
      // {
        PROTOC = lib.getExe buildPackages.protobuf_34;
        NIX_LDFLAGS =
          (baseEnv.NIX_LDFLAGS or "")
          + lib.optionalString stdenv.hostPlatform.isDarwin " -framework CoreFoundation";
      };
  })
