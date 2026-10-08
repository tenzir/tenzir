# Backport NixOS/nixpkgs#555390 at 5ee0d33 until the source-built Bun lands.
final: _prev:
let
  staticCompilerRt = final.llvmPackages_21.compiler-rt;
  staticClang = final.wrapCCWith {
    cc = final.buildPackages.llvmPackages_21.clang-unwrapped;
    bintools = final.stdenv.cc.bintools;
    libc = final.stdenv.cc.libc;
    libcxx = null;
    extraPackages = [
      staticCompilerRt
      final.stdenv.cc.cc
      final.stdenv.cc.cc.lib
    ];
    extraBuildCommands = ''
      resourceRoot="$out/resource-root"
      mkdir "$resourceRoot"
      echo "-resource-dir=$resourceRoot" >> "$out/nix-support/cc-cflags"
      ln -s "${final.buildPackages.llvmPackages_21.clang-unwrapped.lib}/lib/clang/21/include" "$resourceRoot/include"
      ln -s "${staticCompilerRt.out}/lib" "$resourceRoot/lib"
      ln -s "${staticCompilerRt.out}/share" "$resourceRoot/share"
    '';
    nixSupport.cc-cflags = [
      "-rtlib=compiler-rt"
      "-Wno-unused-command-line-argument"
    ];
  };
  llvmPackages = final.buildPackages.llvmPackages_21 // {
    clang = staticClang;
    stdenv = final.overrideCC final.stdenv staticClang;
  };
  bun-unwrapped = final.callPackage ./bun-unwrapped/package.nix (
    final.lib.optionalAttrs final.stdenv.hostPlatform.isStatic { inherit llvmPackages; }
  );
in
{
  inherit bun-unwrapped;
  bun = final.callPackage ./bun/package.nix { };
}
