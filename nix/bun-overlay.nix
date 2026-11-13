# Bun for the monorepo's Nix expressions. Static builds compile it from source,
# backporting NixOS/nixpkgs#555390 at 5ee0d33 until the source-built Bun lands.
# Everything else, including the development shells, uses the release binaries.
final: prev:
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
  bun =
    if final.stdenv.hostPlatform.isStatic then
      final.callPackage ./bun/package.nix { }
    else
      # Bun 1.4.3 fixed `bun test --isolate` and `--parallel` crashing when a
      # finished test file's global was collected while one of its
      # FinalizationRegistry callbacks was still queued. effect's `Atom.family`
      # and `HttpClient` register such callbacks, which crashed the platform's
      # test workers at random. Backports NixOS/nixpkgs@0d765791b093; drop this
      # once the nixpkgs pin carries Bun 1.4.3 or later.
      prev.bun.overrideAttrs (
        finalAttrs: previousAttrs: {
          version = "1.4.4";
          # `src` is picked from `passthru.sources`, which changes below.
          __intentionallyOverridingVersion = true;
          passthru = previousAttrs.passthru // {
            sources =
              let
                release = "https://github.com/oven-sh/bun/releases/download/bun-v${finalAttrs.version}";
              in
              {
                "aarch64-darwin" = final.fetchurl {
                  url = "${release}/bun-darwin-aarch64.zip";
                  hash = "sha256-wkDX8My5DCY6hJ/NsJt+5iYw2SE0jtaq/ahrnVd7xRA=";
                };
                "aarch64-linux" = final.fetchurl {
                  url = "${release}/bun-linux-aarch64.zip";
                  hash = "sha256-uqa7NvUdblhWHgK76oJQOKnfRU9nPW13bou/hvjHhQ4=";
                };
                "x86_64-linux" = final.fetchurl {
                  url = "${release}/bun-linux-x64-baseline.zip";
                  hash = "sha256-4ERutVmVdy65iUvrrppr42jZhai5Jx7hJEhvmUbu3XY=";
                };
              };
          };
        }
      );
}
