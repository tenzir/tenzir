{
  inputs,
}: final: prev: let
  inherit (final) lib;
  inherit (final.stdenv.hostPlatform) isLinux isDarwin isStatic;
  stdenv = final.stdenv;
  overrideAttrsIf = pred: package: f:
    if !pred
    then package
    else package.overrideAttrs f;
in {
  nix2container = inputs.nix2container.packages.x86_64-linux;
  musl = overrideAttrsIf isStatic prev.musl (orig: {
    patches = (orig.patches or []) ++ [
      (prev.buildPackages.fetchpatch {
        name = "musl-strptime-new-format-specifiers";
        url = "https://git.musl-libc.org/cgit/musl/patch?id=fced99e93daeefb0192fd16304f978d4401d1d77";
        hash = "sha256-WhT9C7Mn94qf12IlasVNGXwpR0XnnkFNLDJ6lYx3Xag=";
      })
    ];
  });
  aws-sdk-cpp-tenzir = (final.aws-sdk-cpp.overrideAttrs (previousAttrs: {
    cmakeFlags = previousAttrs.cmakeFlags ++ lib.optionals isDarwin [
      "-DENABLE_TESTING=OFF"
    ];
  })).override {
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
  };
  thrift = if !isStatic then prev.thrift else prev.thrift.overrideAttrs ({
    nativeBuildInputs = [
      prev.pkgsBuildBuild.bison
      prev.pkgsBuildBuild.cmake
      prev.pkgsBuildBuild.flex
      prev.pkgsBuildBuild.pkg-config
      (prev.pkgsBuildBuild.python3.withPackages (ps: [ ps.setuptools ]))
    ];
  });
  rabbitmq-c = prev.rabbitmq-c.overrideAttrs (orig: {
    patches = (orig.patches or []) ++ [
      (prev.fetchpatch2 {
        name = "rabbitmq-c-fix-include-path.patch";
        url = "https://github.com/alanxz/rabbitmq-c/commit/819e18271105f95faba99c3b2ae4c791eb16a664.patch";
        hash = "sha256-/c4y+CvtdyXgfgHExOY8h2q9cJNhTUupsa22tE3a1YI=";
      })
    ];
  });
  restinio = prev.restinio.overrideAttrs (finalAttrs: orig: {
    # Reguires networking to bind to ports on darwin, but keeping it off is
    # safer.
    doCheck = !isDarwin;
    cmakeFlags = orig.cmakeFlags ++ [
      (lib.cmakeBool "RESTINIO_TEST" finalAttrs.doCheck)
    ];
  });
  azure-sdk-for-cpp = prev.callPackage ./azure-sdk-for-cpp { };
  clickhouse-cpp = prev.callPackage ./clickhouse-cpp { };
  arrow-cpp = let
    arrow-cpp' = prev.arrow-cpp.override {
      aws-sdk-cpp-arrow = final.aws-sdk-cpp-tenzir;
      enableGcs = true; # Upstream disabled for darwin.
    };
    arrow-cpp'' = arrow-cpp'.overrideAttrs (orig: {
      nativeBuildInputs = orig.nativeBuildInputs ++ [
        prev.pkgsBuildBuild.pkg-config
      ];
      buildInputs = orig.buildInputs ++ [
        final.azure-sdk-for-cpp
      ];
      cmakeFlags = orig.cmakeFlags ++ [
        "-DARROW_AZURE=ON"
      ];
      installCheckPhase =
        let
          disabledTests = [
            # flaky
            "arrow-flight-test"
            # requires networking
            "arrow-azurefs-test"
            "arrow-gcsfs-test"
            "arrow-flight-integration-test"
          ];
        in
        ''
          runHook preInstallCheck

          ctest -L unittest --exclude-regex '^(${lib.concatStringsSep "|" disabledTests})$'

          runHook postInstallCheck
        '';
    });
  in
    overrideAttrsIf isStatic
    (
      if !isStatic
      then arrow-cpp''
      else
        arrow-cpp''.override {
          enableShared = false;
          google-cloud-cpp = final.google-cloud-cpp.override {
            apis = ["pubsub" "storage"];
          };
        }
    )
    (orig: {
      nativeBuildInputs =
        orig.nativeBuildInputs
        ++ lib.optionals isDarwin [
          (prev.buildPackages.writeScriptBin "libtool" ''
            #!${stdenv.shell}
            if [ "$1" == "-V" ]; then
              echo "Apple Inc. version cctools-1010.6"
              exit 0
            fi
            exec ${lib.getBin prev.buildPackages.darwin.cctools}/bin/${stdenv.cc.targetPrefix}libtool $@
          '')
        ];
      buildInputs = orig.buildInputs ++ [final.sqlite];
      cmakeFlags =
        orig.cmakeFlags
        ++ [
          "-DARROW_BUILD_TESTS=OFF"
          "-DGLOG_SOURCE=SYSTEM"
        ];
      doCheck = false;
      doInstallCheck = false;
      env = {
        NIX_LDFLAGS = lib.optionalString (stdenv.hostPlatform.isStatic && stdenv.hostPlatform.isDarwin)
          "-framework SystemConfiguration";
      };
    });
  arrow-adbc-cpp = prev.callPackage ./arrow-adbc-cpp { };
  arrow-adbc-go = prev.callPackage ./arrow-adbc-go { };
  zeromq =
    if !isStatic
    then prev.zeromq
    else
      prev.zeromq.overrideAttrs (orig: {
        cmakeFlags =
          orig.cmakeFlags
          ++ [
            "-DBUILD_SHARED=OFF"
            "-DBUILD_STATIC=ON"
            "-DBUILD_TESTS=OFF"
          ];
      });
  rdkafka =
    let
      # The FindZLIB.cmake module from CMake breaks when multiple outputs are
      # used.
      zlib = final.zlib.overrideAttrs (orig: {
        outputs = [ "out" ];
        outputDoc = "out";
        postInstall = "";
      });
    in prev.rdkafka.overrideAttrs (orig: {
    nativeBuildInputs = orig.nativeBuildInputs ++ [prev.buildPackages.cmake];
    # The cmake config file doesn't find them if they are not propagated.
    buildInputs = (builtins.filter (x: x.pname != "zlib") orig.buildInputs) ++ [ zlib ];
    cmakeFlags =
      lib.optionals isStatic [
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
  });
  mkStub = name:
    prev.writeShellScriptBin name ''
      echo "stub-${name}: $@" >&2
    '';
  libmaxminddb = overrideAttrsIf isStatic prev.libmaxminddb (orig: {
    nativeBuildInputs = (orig.nativeBuildInputs or []) ++ [prev.buildPackages.cmake];
  });
  fluent-bit = final.callPackage ./fluent-bit {};
  yara =
    if !isStatic
    then prev.yara
    else
      prev.yara.overrideAttrs (orig: {
        NIX_CFLAGS_LINK = "-lz";
      });
  llhttp = prev.llhttp.overrideAttrs (orig: {
    patches = [ 
      (prev.buildPackages.fetchpatch2 {
        name = "llhttp-fix-cmake-pkgconfig-paths.patch";
        url = "https://github.com/nodejs/llhttp/pull/560/commits/9d37252aa424eb9af1d2a83dfa83153bcc0cc27f.patch";
        hash = "sha256-8KsrJsD9orLjZv8mefCMuu8kftKisQ/57lCPK0eiX30=";
      })
    ];
  });
  pfs = final.callPackage ./pfs {};
  uv-bin= final.callPackage ./uv-binary {};
  caf = let
    source = builtins.fromJSON (builtins.readFile ./caf/source.json);
  in
    (prev.caf.override {inherit stdenv;}).overrideAttrs (old:
      {
        # fetchFromGitHub uses ellipsis in the parameter set to be hash method
        # agnostic. Because of that, callPackageWith does not detect that sha256
        # is a required argument, and it has to be passed explicitly instead.
        src = prev.fetchFromGitHub {inherit (source) owner repo rev hash;};
        inherit (source) version;
        # The OpenSSL dependency appears in the interface of CAF, so it has to
        # be propagated downstream.
        propagatedBuildInputs = [final.openssl];
        env.NIX_CFLAGS_COMPILE = "-fno-omit-frame-pointer";
        # Building statically implies using -flto. Since we produce a final binary with
        # link time optimizaitons in Tenzir, we need to make sure that type definitions that
        # are parsed in both projects are the same, otherwise the compiler will complain
        # at the optimization stage.
        # https://github.com/NixOS/nixpkgs/issues/130963
        preCheck = ''
          export LD_LIBRARY_PATH=$PWD/lib
          export DYLD_LIBRARY_PATH=$PWD/lib
        '';
      }
      // lib.optionalAttrs isStatic {
        cmakeFlags =
          old.cmakeFlags
          ++ [
            "-DCAF_BUILD_STATIC=ON"
            "-DCAF_BUILD_STATIC_ONLY=ON"
            "-DCAF_ENABLE_TESTING=OFF"
            "-DOPENSSL_USE_STATIC_LIBS=TRUE"
            "-DCMAKE_POLICY_DEFAULT_CMP0069=NEW"
          ]
          ++ lib.optionals isLinux [
            "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON"
          ];
        hardeningDisable = [
          "fortify"
          "pic"
        ];
        dontStrip = true;
        doCheck = false;
      });
  jemalloc =
    if !isStatic
    then prev.jemalloc
    else
      prev.jemalloc.overrideAttrs (old: {
        EXTRA_CFLAGS = (old.EXTRA_CFLAGS or "") + " -fno-omit-frame-pointer";
        configureFlags = old.configureFlags ++ ["--enable-prof" "--enable-stats"];
        doCheck = !isStatic;
      });
  bats-tenzir = prev.stdenv.mkDerivation {
    pname = "bats-tenzir";
    version = "0.1";
    src = lib.fileset.toSource {
      root = ./../tenzir/bats/lib/bats-tenzir;
      fileset = ./../tenzir/bats/lib/bats-tenzir;
    };
    dontBuild = true;
    installPhase = ''
      mkdir -p "$out/share/bats/bats-tenzir"
      cp load.bash "$out/share/bats/bats-tenzir"
      cp -r src "$out/share/bats/bats-tenzir"
    '';
    meta = {
      platforms = lib.platforms.all;
      license = lib.licenses.bsd3;
      #maintainers = [ ];
    };
  };
  bundledPlugins = builtins.attrNames (lib.filterAttrs (name: type: type == "directory") (builtins.readDir ../plugins));
  integration-test-tree = lib.fileset.unions ([
    (lib.fileset.difference ../tenzir/bats ../tenzir/bats/lib/bats-tenzir)
  ] ++ builtins.map (x: lib.fileset.maybeMissing (./.. + "/plugins/${x}/bats")) final.bundledPlugins);
  tenzir-tree = lib.fileset.difference (lib.fileset.unions [
    ../changelog
    ../cmake
    ../libtenzir
    ../libtenzir_test
    ../plugins
    ../python
    ../schema
    ../scripts
    ../tenzir
    ../VERSIONING.md
    ../CMakeLists.txt
    ../LICENSE
    ../README.md
    ../tenzir.spdx.json
    ../VERSIONING.md
    ../tenzir.yaml.example
    ../version.json
  ]) final.integration-test-tree;
  tenzir-source = lib.fileset.toSource {
    root = ./..;
    fileset = final.tenzir-tree;
  };
  unchecked = {
    tenzir-de = final.callPackage ./tenzir {
      inherit stdenv;
      isReleaseBuild = inputs.isReleaseBuild.value;
    };
    # Policy: The suffix-less `tenzir' packages come with a few closed source
    # plugins.
    tenzir = let
      tenzir-plugins-source =
        if builtins.pathExists ./../contrib/tenzir-plugins/README.md
          then ./../contrib/tenzir-plugins
          else prev.callPackage ./tenzir/plugins/source.nix {};
      pkg = final.unchecked.tenzir-de.override {
        inherit tenzir-plugins-source;
      };
    in
      pkg.withPlugins (ps: [
        ps.compaction
        ps.context
        ps.packages
        ps.pipeline-manager
        ps.platform
        ps.to_asl
        ps.to_azure_log_analytics
        ps.to_splunk
        ps.to_google_secops
        ps.to_google_cloud_logging
        ps.vast
      ] ++ lib.optionals (!isStatic) [
        ps.snowflake
      ]);
  };
  toChecked =
    x:
    # Run checks only on Linux for now. Alternative platforms are expensive in
    # CI and also not as important.
    if isLinux then
      final.callPackage ./tenzir/check.nix
        {
          src = lib.fileset.toSource {
            root = ../.;
            fileset = lib.fileset.unions [
              final.integration-test-tree
              ../tenzir.yaml.example
            ];
          };
        }
        x
    else
      x // { unchecked = x; };
  tenzir-de = final.toChecked final.unchecked.tenzir-de;
  tenzir = final.toChecked final.unchecked.tenzir;
  tenzir-integration-test-runner = with prev.pkgsBuildBuild; [
    (bats.withLibraries (
      p: [
        p.bats-support
        p.bats-assert
        bats-tenzir
      ]
    ))
    parallel
  ];
  tenzir-integration-test-deps = with prev.pkgsBuildBuild; [
    curl
    jq
    lsof
    perl
    procps
    socat
    # toybox provides a portable `rev`, but it also comes with a `cp` that does
    # not provide all the flags that are used in stdenv phases. We just add it
    # to the PATH in the checkPhase directly as a workaround.
    #toybox
    yara
    (python3.withPackages (ps:
      with ps; [
        trustme
      ])
    )
  ] ++ final.tenzir-integration-test-runner;
  pythonPackagesExtensions =
    prev.pythonPackagesExtensions
    ++ [
      (
        python-final: python-prev: {
          dynaconf = python-final.callPackage ./dynaconf {};
        }
      )
    ];
  speeve = final.buildGoModule rec {
    pname = "speeve";
    version = "0.1.3";
    vendorHash = "sha256-Mw1cRIwmDS2Canljkuw96q2+e+z14MUcU5EtupUcTDQ=";
    src = final.fetchFromGitHub {
      rev = "v${version}";
      owner = "satta";
      repo = pname;
      hash = "sha256-75QrtuOduUNT9g2RJRWUow8ESBqsDDXCMGVNQKFc+SE=";
    };
    # upstream does not provide a go.sum file
    preBuild = ''
      cp ${./speeve-go.sum} go.sum
    '';
    subPackages = ["cmd/speeve"];
  };
}
