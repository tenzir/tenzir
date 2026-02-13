{
  fizz,
  fetchFromGitHub,
}:
let
  facebookNetworkStack = import ../facebook-network-stack.nix;
in
fizz.overrideAttrs (_: {
  version = facebookNetworkStack.release;
  src = fetchFromGitHub {
    inherit (facebookNetworkStack.fizz)
      owner
      repo
      rev
      hash
      ;
  };
})
