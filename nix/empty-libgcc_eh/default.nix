{
  stdenv,
  runCommand,
  pkgsBuildHost,
}:
# Work around https://github.com/NixOS/nixpkgs/issues/177129.
# Idea taken from lix / scals.
runCommand "empty-libgcc_eh" { } ''
  if $CC -Wno-unused-command-line-argument -x c - -o /dev/null <<< 'int main() {}'; then
    echo "linking succeeded; please remove empty-gcc-eh workaround" >&2
    exit 3
  fi
  mkdir -p $out/lib
  ${pkgsBuildHost.binutils-unwrapped}/bin/${stdenv.cc.targetPrefix}ar r $out/lib/libgcc_eh.a
''
