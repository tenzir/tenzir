Build the tenzir binary. Follow the steps below.

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

# Build Configuration

By default, use `-DCMAKE_BUILD_TYPE=RelWithDebInfo` to set the release type.

# Final Instructions

These are custom instructions added by the user. They take precedence over
everything written above:

<instructions>
${ARGS}
</instructions>

In the future, 