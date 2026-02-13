{
  fetchFromGitHub,
  mvfst,
}:
let
  facebookNetworkStack = import ../facebook-network-stack.nix;
in
mvfst.overrideAttrs (orig: {
  version = facebookNetworkStack.release;
  src = fetchFromGitHub {
    inherit (facebookNetworkStack.mvfst)
      owner
      repo
      rev
      hash
      ;
  };
  patches = orig.patches or [ ];
  postPatch = ''
    # Keep upstream install hook, but the DSR directory does not exist in all
    # mvfst snapshots.
    printf 'install(TARGETS mvfst_test_utils)\n' >> quic/common/test/CMakeLists.txt
    if [ -f quic/dsr/CMakeLists.txt ]; then
      printf 'install(TARGETS mvfst_dsr_backend)\n' >> quic/dsr/CMakeLists.txt
    fi
  '';
})
