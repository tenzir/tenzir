{ runCommand, git}:
src:
runCommand "gitDescribe.out" {} ''
  cd ${src}
  echo -n "$(${git.nativeDrv}/bin/git describe --tags --long --dirty)" > $out
''
