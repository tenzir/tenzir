rec {
  combine = fs: attrs: builtins.concatLists (map (f: f attrs) fs);

  build =
    ps: with ps; [
      setuptools
      pip
    ];

  runtime =
    ps: with ps; [
      aiohttp
      dynaconf
      numpy
      pandas
      pyarrow
      python-box
    ];

  integration =
    ps: with ps; [
      boto3
      googleapis-common-protos
      grpcio
      grpcio-tools
      pyarrow
      python-box
      pyzmq
      trustme
    ];

  integration-container =
    ps: with ps; [
      pymysql
      # PyIceberg's package tests expect older PyArrow string types, but its
      # runtime is compatible with the pinned PyArrow used by these tests.
      (pyiceberg.overridePythonAttrs (_: {
        doCheck = false;
      }))
    ];

  dev = combine [
    build
    runtime
    integration
    integration-container
    (
      ps: with ps; [
        boto3-stubs
      ]
    )
  ];
}
