{ runCommand, git }:
src:
let
  git_ = git.nativeDrv or git;
in
runCommand "gitDescribe.out" {} ''
  cd ${src}
  echo -n "$(${git_}/bin/git describe --abbrev=10 --long --dirty --match='v[0-9]*')" > $out
''
