# Nix

## Development

Enter the development shell before running the project's pinned tools:

```sh
nix develop
```

## Dependencies

Add direct dependencies to the appropriate input list in
`nix/dependencies.nix`. Add a package to `nix/overlay.nix` only when Nixpkgs
does not provide it or when Tenzir needs to override it.

## Build packages

The flake exposes these common package outputs:

- `nix build .#tenzir` builds the edition with additional closed-source plugins.
- `nix build .#tenzir-de` builds the developer edition without those plugins.
- `nix build .#tenzir-static` builds a musl-linked static binary on Linux.
- `nix build .#tenzir-static^package` builds the installable package artifacts:
  DEB, RPM, and tar.gz files on Linux, or a `.pkg` file on macOS.
