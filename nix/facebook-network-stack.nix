# Pinned sources for the lock-stepped Facebook network stack (folly, fizz,
# wangle, mvfst, proxygen). All libraries track a single weekly release.
#
# NOTE: the `hash` values below are placeholders (lib.fakeHash). They are
# filled in by CI on the first Nix build from the reported "got:" hash.
{
  release = "2026.06.29.00";
  folly = {
    owner = "tenzir";
    repo = "folly";
    rev = "c3940fe3ff0fd02a82497cf6411a9ebc5ed41bde";
    hash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  };
  fizz = {
    owner = "facebookincubator";
    repo = "fizz";
    rev = "8f1c4803666e9becb6f5917ecfe449d80ef28b5e";
    hash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  };
  wangle = {
    owner = "facebook";
    repo = "wangle";
    rev = "74b02bbfb8237c1ada5c0c592a535fd3b90ef460";
    hash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  };
  mvfst = {
    owner = "facebook";
    repo = "mvfst";
    rev = "0564d5c495cc3281d79d70563fd9e5d57dadf050";
    hash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  };
  proxygen = {
    owner = "tenzir";
    repo = "proxygen";
    rev = "ccf1e5559d8bf378a8e9cdf4669609f81550a552";
    hash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  };
}
