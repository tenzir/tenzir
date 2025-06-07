# This is callPackageWith without applying makeOverridable to fn.
{ lib }:
autoArgs: fn: args:
let
  f = if lib.isFunction fn then fn else import fn;
  fargs = lib.functionArgs f;

  # All arguments that will be passed to the function
  # This includes automatic ones and ones passed explicitly
  allArgs = lib.intersectAttrs fargs autoArgs // args;

  # a list of argument names that the function requires, but
  # wouldn't be passed to it
  missingArgs =
    # Filter out arguments that have a default value
    (
      lib.filterAttrs (name: value: !value)
        # Filter out arguments that would be passed
        (removeAttrs fargs (lib.attrNames allArgs))
    );

  # Get a list of suggested argument names for a given missing one
  getSuggestions =
    arg:
    lib.pipe (autoArgs // args) [
      lib.attrNames
      # Only use ones that are at most 2 edits away. While mork would work,
      # levenshteinAtMost is only fast for 2 or less.
      (lib.filter (lib.levenshteinAtMost 2 arg))
      # Put strings with shorter distance first
      (lib.sortOn (lib.levenshtein arg))
      # Only take the first couple results
      (lib.take 3)
      # Quote all entries
      (map (x: "\"" + x + "\""))
    ];

  prettySuggestions =
    suggestions:
    if suggestions == [ ] then
      ""
    else if lib.length suggestions == 1 then
      ", did you mean ${lib.elemAt suggestions 0}?"
    else
      ", did you mean ${lib.concatStringsSep ", " (lib.init suggestions)} or ${lib.last suggestions}?";

  errorForArg =
    arg:
    let
      loc = builtins.unsafeGetAttrPos arg fargs;
      # loc' can be removed once lib/minver.nix is >2.3.4, since that includes
      # https://github.com/NixOS/nix/pull/3468 which makes loc be non-null
      loc' =
        if loc != null then
          loc.file + ":" + toString loc.line
        else if !lib.isFunction fn then
          toString fn + lib.optionalString (lib.pathIsDirectory fn) "/default.nix"
        else
          "<unknown location>";
    in
    "Function called without required argument \"${arg}\" at "
    + "${loc'}${prettySuggestions (getSuggestions arg)}";

  # Only show the error for the first missing argument
  error = errorForArg (lib.head (lib.attrNames missingArgs));

in
if missingArgs == { } then
  f allArgs
# This needs to be an abort so it can't be caught with `builtins.tryEval`,
# which is used by nix-env and ofborg to filter out packages that don't evaluate.
# This way we're forced to fix such errors in Nixpkgs,
# which is especially relevant with allowAliases = false
else
  abort "lib.customisation.callFunctionWith: ${error}"
