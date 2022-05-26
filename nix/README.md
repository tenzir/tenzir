# Nix build support facilities

## Development Environment

The dependencies for a dynamic built can be fetched with
```sh
nix develop
```

The `direnv` tool has good support for nix and can be used with an `.envrc` with
the content:
```
use flake
```

If you want to silence the messages about binary caches you can use a variation
that invokes `nix` with a lower verbosity setting:
```
use_flake2() {
  watch_file flake.nix
  watch_file flake.lock
  mkdir -p "$(direnv_layout_dir)"
  eval "$(nix --quiet --quiet print-dev-env --profile "$(direnv_layout_dir)/flake-profile" "$@")"
}

use_flake2
```

The VAST repository comes with a set of CMake configure and build presets for
Nix based builds:
* `nix-clang-debug`
* `nix-clang-redeb`
* `nix-clang-release`
* `nix-gcc-debug`
* `nix-gcc-redeb`
* `nix-gcc-release`

### Static Builds

Static binaries require a that the dependencies were built in static mode as
well. That means we need to use a different environment, you can enter it with

```sh
nix develop .#vast-static
```

The CMake presets for that mode are
* `nix-gcc-static-debug`
* `nix-gcc-static-redeb`
* `nix-gcc-static-release`
