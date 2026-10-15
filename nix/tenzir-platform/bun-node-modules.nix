# Builds a hoisted `node_modules` tree from `bun.lock` without a fixed-output
# derivation for the whole tree.
#
# Every registry package in `bun.lock` carries an SRI integrity hash, so each
# tarball becomes its own small fixed-output `fetchurl`. The lockfile keys
# already encode the hoisted install location (`a/b` is `node_modules/a/
# node_modules/b`), so the tree is assembled directly from them.
{
  lib,
  stdenvNoCC,
  fetchurl,
  gnutar,
  gzip,
  patch,
}:

{
  pname,
  src,
  # Install only the closure of the root `dependencies`, like
  # `bun install --production`.
  production ? true,
}:

let
  # `bun.lock` is JSON with trailing commas, which `builtins.fromJSON` rejects.
  # Package names and versions never contain `,` followed by a closing
  # bracket, so a textual rewrite is sufficient.
  stripTrailingCommas =
    text:
    lib.concatMapStrings (chunk: if builtins.isList chunk then builtins.head chunk else chunk) (
      builtins.split ",([[:space:]]*[]}])" text
    );

  lock = builtins.fromJSON (stripTrailingCommas (builtins.readFile (src + "/bun.lock")));

  root = lock.workspaces."";

  # Entries are `[ "name@version" registry meta integrity ]` for registry
  # packages. Other resolutions (git, file, workspace) are not supported.
  parseEntry =
    key: entry:
    let
      ident = builtins.elemAt entry 0;
      at = lib.lists.findFirstIndex (c: c == "@") null (lib.drop 1 (lib.stringToCharacters ident));
      name = builtins.substring 0 (at + 1) ident;
      version = builtins.substring (at + 2) (-1) ident;
    in
    assert lib.assertMsg (
      builtins.length entry == 4 && builtins.elemAt entry 1 == ""
    ) "bun.lock: unsupported resolution for ${key}";
    {
      inherit key name version;
      meta = builtins.elemAt entry 2;
      integrity = builtins.elemAt entry 3;
      url = "https://registry.npmjs.org/${name}/-/${baseNameOf name}-${version}.tgz";
    };

  packages = lib.mapAttrs parseEntry lock.packages;

  # Node's resolution algorithm over lockfile keys: look for `dep` next to the
  # requiring package, then in each ancestor directory, then at the top level.
  resolve =
    fromKey: dep:
    let
      go =
        prefix:
        let
          candidate = if prefix == "" then dep else "${prefix}/${dep}";
        in
        if packages ? ${candidate} then
          candidate
        else if prefix == "" then
          null
        else
          go (parentKey prefix);
      # Strips the last package name, which spans two segments when scoped.
      parentKey =
        key:
        let
          parts = lib.splitString "/" key;
          n = builtins.length parts;
          drop = if n >= 2 && lib.hasPrefix "@" (builtins.elemAt parts (n - 2)) then 2 else 1;
        in
        lib.concatStringsSep "/" (lib.take (n - drop) parts);
    in
    go fromKey;

  nodePlatform =
    {
      linux = "linux";
      darwin = "darwin";
    }
    .${stdenvNoCC.hostPlatform.parsed.kernel.name};
  nodeCpu =
    {
      x86_64 = "x64";
      aarch64 = "arm64";
    }
    .${stdenvNoCC.hostPlatform.parsed.cpu.name};
  matches =
    value: wanted:
    let
      values = lib.toList value;
      negated = builtins.filter (lib.hasPrefix "!") values;
    in
    value == null
    || builtins.elem wanted values
    || (negated != [ ] && !(builtins.elem "!${wanted}" values) && negated == values);
  supported =
    pkg: matches (pkg.meta.os or null) nodePlatform && matches (pkg.meta.cpu or null) nodeCpu;

  # Required edges of a package: regular dependencies plus non-optional peers,
  # which Bun installs automatically.
  edges = meta: {
    required =
      lib.attrNames (meta.dependencies or { })
      ++ lib.subtractLists (meta.optionalPeers or [ ]) (lib.attrNames (meta.peerDependencies or { }));
    optional = lib.attrNames (meta.optionalDependencies or { });
  };

  closure =
    let
      step =
        fromKey: meta:
        let
          e = edges meta;
          resolveAll = deps: builtins.filter (k: k != null) (map (resolve fromKey) deps);
        in
        map (key: { inherit key; }) (
          resolveAll e.required ++ builtins.filter (k: supported packages.${k}) (resolveAll e.optional)
        );
      rootMeta =
        if production then
          {
            inherit (root) dependencies;
            optionalDependencies = root.optionalDependencies or { };
          }
        else
          root // { dependencies = (root.dependencies or { }) // (root.devDependencies or { }); };
    in
    map (x: packages.${x.key}) (
      builtins.genericClosure {
        startSet = step "" rootMeta;
        operator = { key }: step key packages.${key}.meta;
      }
    );

  tarballs = map (pkg: {
    inherit pkg;
    tarball = fetchurl {
      inherit (pkg) url;
      hash = pkg.integrity;
    };
  }) closure;

  # Lockfile keys separate nested packages with `/`, but scoped names contain
  # one as well. Rebuild the path from the resolved chain of package names.
  pathOf =
    key:
    let
      parts = lib.splitString "/" key;
      names =
        lib.foldl'
          (
            acc: part:
            if acc.pending != null then
              {
                names = acc.names ++ [ "${acc.pending}/${part}" ];
                pending = null;
              }
            else if lib.hasPrefix "@" part then
              acc // { pending = part; }
            else
              acc // { names = acc.names ++ [ part ]; }
          )
          {
            names = [ ];
            pending = null;
          }
          parts;
    in
    lib.concatMapStringsSep "/" (n: "node_modules/${n}") names.names;

  patches = lib.mapAttrs (_: file: src + "/${file}") (lock.patchedDependencies or { });
in
stdenvNoCC.mkDerivation {
  inherit pname;
  version = "lock";

  dontUnpack = true;
  dontConfigure = true;
  dontFixup = true;
  nativeBuildInputs = [
    gnutar
    gzip
    patch
  ];

  buildPhase = ''
    runHook preBuild
    ${lib.concatMapStrings (
      { pkg, tarball }:
      ''
        mkdir -p ${lib.escapeShellArg (pathOf pkg.key)}
        tar -xzf ${tarball} -C ${lib.escapeShellArg (pathOf pkg.key)} \
          --strip-components=1 --no-same-owner --no-same-permissions
      ''
      + lib.optionalString (patches ? "${pkg.name}@${pkg.version}") ''
        patch -d ${lib.escapeShellArg (pathOf pkg.key)} -p1 -F0 \
          < ${patches."${pkg.name}@${pkg.version}"}
      ''
    ) tarballs}
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    mkdir -p "$out"
    cp -R node_modules "$out/"
    runHook postInstall
  '';

  passthru = {
    inherit closure;
  };
}
