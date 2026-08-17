{
  lib,
  stdenv,
  gmp,
}:
gmp.overrideAttrs (
  orig:
  lib.optionalAttrs (with stdenv.hostPlatform; isStatic && isAarch && isLinux) {
    # The makeStaticLibraries adapter appends `CFLAGS=-fPIC` on aarch64-linux,
    # clobbering the `CFLAGS=-std=c99` that gmp's configure requires with
    # GCC >= 15 (which defaults to C23). Re-assign both flags together via
    # configureFlagsArray, which the generic builder passes after
    # configureFlags so that the merged assignment wins. The plain
    # configureFlags list cannot hold values with spaces.
    preConfigure = (orig.preConfigure or "") + ''
      configureFlagsArray+=("CFLAGS=-std=c99 -fPIC")
    '';
  }
)
