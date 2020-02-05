{ runCommand, git}:
src:
runCommand "gitDescribe.out" {} ''
  cd ${src}
  echo -n "$(${git}/bin/git describe --tags --long --dirty)" > $out
''
