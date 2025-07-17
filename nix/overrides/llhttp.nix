{
  llhttp,
  fetchpatch2,
}:
llhttp.overrideAttrs (orig: {
  patches = [
    (fetchpatch2 {
      name = "llhttp-fix-cmake-pkgconfig-paths.patch";
      url = "https://github.com/nodejs/llhttp/pull/560/commits/9d37252aa424eb9af1d2a83dfa83153bcc0cc27f.patch";
      hash = "sha256-8KsrJsD9orLjZv8mefCMuu8kftKisQ/57lCPK0eiX30=";
    })
  ];
})
