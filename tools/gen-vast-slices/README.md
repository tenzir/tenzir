# gen-vast-slices

The **gen-vast-slices** tool parses supported log formats from STDIN into table
slices and prints them in serialized form to STDOUT.

Our primary use case is to create a `.cpp` with an array that contains the
serialized form of a table slice vector. Unit tests than can quickly
deserialize the vector instead of parsing files from the file system.

## Usage

We use this tool primarily for automatic code generation via CMake.

By default, the tool reads from STDIN and writes to STDOUT. When generating C++
source code (the default mode), it's best to pipe the output through
`clang-format` when generating code under the Git source tree. Also, explicitly
defining namespace and variable names for the generated C++ source code is good
practice:

    gen-vast-slices --namespace-name="artifacts::logs::zeek" \
                    --variable-name="small_con_buf" \
                    < libvast_test/artifacts/logs/zeek/small_conn.log \
                    | clang-format
