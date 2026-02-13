{
  fetchFromGitHub,
  wangle,
}:
let
  facebookNetworkStack = import ../facebook-network-stack.nix;
in
wangle.overrideAttrs (_: {
  version = facebookNetworkStack.release;
  src = fetchFromGitHub {
    inherit (facebookNetworkStack.wangle)
      owner
      repo
      rev
      hash
      ;
  };
})
