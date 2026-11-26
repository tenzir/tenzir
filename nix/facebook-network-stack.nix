{
  release = "2026.06.29.00";
  cxxStandard = "23";
  # x86 SIMD level for the Facebook libraries. It must match the engine's
  # TENZIR_ENABLE_*_INSTRUCTIONS options: folly's F14 hash tables check at link
  # time that every translation unit agrees on SSE4.2 availability. AVX and
  # AVX2 stay off so that release builds run on CPUs without them, such as
  # Intel Ivy Bridge.
  x86CxxFlags = "-msse -msse2 -msse3 -mssse3 -msse4.1 -msse4.2";
  folly = {
    owner = "tenzir";
    repo = "folly";
    rev = "0314f22551034e6acbf894d531a140918d7709ce";
    hash = "sha256-0LAaKMVavXbeqSFMoPr0AdBPC2Wj4HsDyGwoiEc6V4k=";
  };
  fizz = {
    owner = "facebookincubator";
    repo = "fizz";
    rev = "8f1c4803666e9becb6f5917ecfe449d80ef28b5e";
    hash = "sha256-JS7UX/ilrFV1BvdW45uj+MGz6PdhKNh162es/khQOMQ=";
  };
  wangle = {
    owner = "facebook";
    repo = "wangle";
    rev = "74b02bbfb8237c1ada5c0c592a535fd3b90ef460";
    hash = "sha256-4upeE8XiVPWb19y6DL+GGKkxcFDdvOfUjGoy3LkqNwM=";
  };
  mvfst = {
    owner = "tenzir";
    repo = "mvfst";
    rev = "27bea101b7bd41ebadf1f713bd65ade44a64ab80";
    hash = "sha256-5KeDgBgrr7GhmEOMLWXTkPXMXdpcqTGOTIh0VIRJrMo=";
  };
  proxygen = {
    owner = "tenzir";
    repo = "proxygen";
    rev = "f14fa8382cea6ca2d055593fa480cc9553a54c87";
    hash = "sha256-/QM4LycKu0fmIwAYYpCmh5zIT5iwrE3LCSiDn3OlCOA=";
  };
}
