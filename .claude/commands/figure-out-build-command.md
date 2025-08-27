Your task is to prepare the tenzir build for this worktree.

Follow the steps below.

# Build types

If the instructions don't specify the build type, assume a local build
was intended.

## Local build

1. If you don't know the build directory, look for an existing build
   directory named `build/`.
   If you cannot find an existing build directory, create a new one with
   the command

        ```
        cmake -B build/ [ARGS]
        ```

   See the "Build Configuration" below for details on what args should be passed.

2. Build the project using one of these commands:
   - To build only the tenzir binary, use `cmake --build <build dir> --target tenzir`
   - To build everything, use `cmake --build <build dir>`.
   By default, build only the tenzir binary.
   Always run the build command with an unlimited timeout, since it can take
   a very long time.

## Docker build
   - ... create a resource-constrained worker
   - To make a docker build, use `docker build . -worker`

## Nix build
 - 



# Build Configuration

By default, use `-DCMAKE_BUILD_TYPE=RelWithDebInfo` to set the release type.

To build only a subset of plugins, use the `-DTENZIR_PLUGINS=a,b,c` option.

# Final Instructions

These are custom instructions added by the user. They take precedence over
everything written above:

<instructions>
${ARGS}
</instructions>

Add the correct build invocation to `CLAUDE.local.md`.
Never commit this change, because `CLAUDE.local.md` is not tracked by git.
