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
      datetime
      pyarrow
      python-box
      pyzmq
      trustme
    ];

  integration-container =
    ps: with ps; [
      pymysql
    ];

  dev = combine [
    build
    runtime
    integration
    integration-container
    (ps: with ps; [
      boto3
      boto3-stubs
    ])
  ];
}
