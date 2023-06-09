# Install

The installation phase takes a build artifact and natively integrates it into
the operating system. We currently support the following operating systems:

1. [Linux](install/linux)
2. [macOS](install/macos)

## Executables

When installing the build artifacts, you end up with the following key
executables:

1. `tenzir`: run a pipeline
2. `tenzir-node`: spawn a node

There is also `tenzir-ctl` as "master" executable to invoke various commands.
This is a temporary solution that we plan to sunset in the future, after we
migrated its functionality into (1) and (2).
