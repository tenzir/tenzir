{
  lib,
  stdenv,
  rdkafka,
  zlib,
  pkgsBuildBuild,
}:
let
  # The FindZLIB.cmake module from CMake breaks when multiple outputs are
  # used.
  zlib_singleout = zlib.overrideAttrs (orig: {
    outputs = [ "out" ];
    outputDoc = "out";
    postInstall = "";
  });
in
rdkafka.overrideAttrs (orig: {
  nativeBuildInputs = orig.nativeBuildInputs ++ [ pkgsBuildBuild.cmake ];
  # The cmake config file doesn't find them if they are not propagated.
  buildInputs = (builtins.filter (x: x.pname != "zlib") orig.buildInputs) ++ [ zlib_singleout ];
  cmakeFlags =
    lib.optionals stdenv.hostPlatform.isStatic [
      "-DRDKAFKA_BUILD_STATIC=ON"
      # The interceptor tests library is hard-coded to SHARED.
      "-DRDKAFKA_BUILD_TESTS=OFF"
    ]
    ++ lib.optionals stdenv.cc.isClang [
      "-DRDKAFKA_BUILD_TESTS=OFF"
    ];

  postFixup = lib.optionalString stdenv.hostPlatform.isStatic ''
    for pc in rdkafka{,++}; do
      ln -s $out/lib/pkgconfig/$pc{-static,}.pc
    done
  '';
})
