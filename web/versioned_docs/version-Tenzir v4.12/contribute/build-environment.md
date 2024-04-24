---
sidebar_position: 3
---

# Build Environment

## Use Nix as Reproducible Development Environment

The dependencies for a dynamic build can be fetched by running `nix develop`
from the topmost directory in the source tree.

The [direnv][direnv] tool is able to automate this process, create an `.envrc`
with the content:

```
use flake
```

and it will automatically add the dependencies to your shell environment when
you move into the source directory.

If you want to silence the messages about binary caches you can use a variation
of `.envrc` that invokes `nix` with a lower verbosity setting:

```
use_flake2() {
  watch_file flake.nix
  watch_file flake.lock
  mkdir -p "$(direnv_layout_dir)"
  eval "$(nix --quiet --quiet print-dev-env --profile "$(direnv_layout_dir)/flake-profile" "$@")"
}

use_flake2
```

The Tenzir repository comes with a set of CMake configure and build presets that
can be used in this environment:

- `nix-clang-debug`
- `nix-clang-redeb`
- `nix-clang-release`
- `nix-gcc-debug`
- `nix-gcc-redeb`
- `nix-gcc-release`

:::note
This build environment is currently only tested on Linux.
:::

### Static Builds

Static binaries require a that the dependencies were built in static mode as
well. That means we need to use a different environment, you can enter it with

```sh
nix develop .#tenzir-static
```

The CMake presets for that mode are:

- `nix-gcc-static-debug`
- `nix-gcc-static-redeb`
- `nix-gcc-static-release`

[direnv]: https://direnv.net/
