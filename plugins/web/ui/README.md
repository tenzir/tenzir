# VAST UI

### Dependencies

The only dependencies are nodejs (version 18.x) and the Yarn package manager.

To get a nix shell with these, run the following

```fish
# For a flake enabled nix setup
nix shell nixpkgs#nodejs-18_x nixpkgs#yarn

# Otherwise
nix-shell -p nodejs-18_x yarn
```

### Develop

To develop the UI independently from VAST itself:

1. Install the nodejs dependencies

```fish
yarn install
```

2. You need a running VAST instance with the web server enabled in the dev mode.
   For example

```fish
vast start '--commands="web server --mode=dev"'
```

3. (optional) If your VAST server's REST API endpoint is different from the
   default (`http://localhost:42001/api/v0`), copy
   the `.env.example` to `.env` and edit that.

4. Run the dev server

```fish
yarn run dev
```
