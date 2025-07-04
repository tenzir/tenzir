{
  lib,
  buildPythonPackage,
  fetchPypi,
  python,
  pythonOlder,
  pytestCheckHook,
  setuptools,
  flask,
  django,
  iniSupport ? false,
  configobj,
  redisSupport ? false,
  redis,
  vaultSupport ? false,
  hvac,
}:
buildPythonPackage rec {
  pname = "dynaconf";
  version = "3.2.3";
  format = "setuptools";

  disabled = pythonOlder "3.8";
  doCheck = false;

  src = fetchPypi {
    inherit pname version;
    sha256 = "sha256-ije6OxbfZMsds4PqrZxzOuziGEE74PrV14X3qQdhIQY=";
  };

  propagatedBuildInputs =
    [ setuptools ]
    ++ lib.optional iniSupport configobj
    ++ lib.optional redisSupport redis
    ++ lib.optional vaultSupport hvac;

  nativeCheckInputs = [
    pytestCheckHook
    configobj
    flask
    django
  ];

  disabledTestPaths = [
    "tests/test_redis.py"
    "tests/test_vault.py"
  ];

  pythonImportsCheck = [
    "dynaconf"
  ];

  meta = with lib; {
    description = "The dynamic configurator for your Python Project";
    homepage = "https://github.com/dynaconf/dynaconf";
    license = licenses.mit;
    maintainers = [ ];
  };
}
