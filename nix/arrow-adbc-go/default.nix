{
  lib,
  stdenv,
  buildGoModule,
  fetchFromGitHub,
}:
buildGoModule (
  {
    pname = "arrow-adbc-go";
    version = "1.3.0";

    src = fetchFromGitHub {
      owner = "apache";
      repo = "arrow-adbc";
      rev = "apache-arrow-adbc-15";
      hash = "sha256-QRWVmUYNdMxQqe9dIBxcY8pY8aAbKIh3dhX3rzCGYI4=";
    };

    sourceRoot = "source/go/adbc";

    proxyVendor = true;

    vendorHash = "sha256-+hUYaFvmySnz2rzDszejcwzoVoCe1lAoj8qNwfMEfp4=";

    postUnpack = ''
      rm -rf source/go/adbc/driver/flightsql/cmd
      rm -rf source/go/adbc/driver/bigquery
      rm -rf source/go/adbc/pkg/bigquery
    '';

    #subPackages = [
    #  "driver/snowflake/..."
    #];

    tags = [
      "driverlib"
    ];

    env = {
      GOBIN = "${placeholder "out"}/lib";
      NIX_DEBUG = 3;
    };

    #GOFLAGS = [
    #  "-shared"
    #];

    ldflags =
      [
        "-s"
        "-w"
      ]
      ++ (
        if stdenv.hostPlatform.isStatic then
          [
            "-buildmode=c-archive"
            "-extar=${stdenv.cc.targetPrefix}ar"
          ]
        else
          [
            "-buildmode=c-shared"
          ]
      );
    #++ [ "-buildmode=c-archive" ];

    doCheck = false;

    postInstall = lib.optionalString stdenv.hostPlatform.isStatic ''
      for f in $out/lib/*; do
        mv $f $f.a
        chmod -x $f.a
      done
    '';

    meta = {
      description = "Database connectivity API standard and libraries for Apache Arrow";
      homepage = "https://arrow.apache.org/adbc/";
      license = lib.licenses.asl20;
      platforms = lib.platforms.unix;
      maintainers = [ lib.maintainers.tobim ];
    };
  }
  // lib.optionalAttrs (!stdenv.hostPlatform.isStatic) {
    buildPhase = ''
      runHook preBuild

      if [ -z "$enableParallelBuilding" ]; then
        export NIX_BUILD_CORES=1
      fi
      cd pkg/snowflake
      go build -tags=driverlib -buildmode=c-shared -o snowflake.so -v -p $NIX_BUILD_CORES .

      runHook postBuild
    '';
    checkPhase = ''
      runHook preCheck

      go test -v -p $NIX_BUILD_CORES .

      runHook postCheck
    '';
    installPhase = ''
      runHook preInstall

      mkdir -p $out/lib
      cp snowflake.so $out/lib

      runHook postInstall
    '';
  }
)
