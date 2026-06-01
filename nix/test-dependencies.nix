{
  python3Packages,
  fetchFromGitHub,
  curl,
  jq,
  lsof,
  perl,
  procps,
  socat,
  yara,
  uv,
  parallel,
  openssl,
}:
rec {
  tenzir-test = python3Packages.buildPythonPackage (finalAttrs: {
    pname = "tenzir-test";
    version = "1.10.2";
    pyproject = true;

    src = fetchFromGitHub {
      owner = "tenzir";
      repo = "test";
      tag = "v${finalAttrs.version}";
      hash = "sha256-02W4vP6cAGANzCtIwF9N/r5OKBGz+11yyINC35Nqcwc=";
    };

    build-system = with python3Packages; [ hatchling ];

    dependencies = with python3Packages; [
      click
      pyyaml
    ];

    # `tenzir-test` itself is already wrapped with its direct dependencies, but
    # it also spawns plain Python subprocesses (for example via
    # `python -m tenzir_test._python_runner`). Those subprocesses do not inherit
    # the package's embedded Python path, so add the site-packages they need
    # here as well.
    postFixup = ''
      wrapProgram $out/bin/tenzir-test \
        --prefix PYTHONPATH : "$out/${python3Packages.python.sitePackages}:${
          python3Packages.makePythonPath [
            python3Packages.click
            python3Packages.pyyaml
            python3Packages.boto3
            python3Packages.pyarrow
            python3Packages.python-box
            python3Packages.pymysql
            python3Packages.pyzmq
            python3Packages.trustme
          ]
        }"
    '';
  });

  pythonPkgsFn = ps: [
    ps.trustme
    ps.pymysql
    ps.pyzmq
  ];

  tenzir-integration-test-deps = [
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
    uv
    parallel
    openssl
    tenzir-test
  ];
}
