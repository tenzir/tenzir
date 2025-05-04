# Tenzir Integration Tests

This directory contains a minimal integration testing framework for Tenzir
pipelines. It uses the `run.py` script to execute Tenzir Query Language (TQL)
files and compare their output against reference files.

## Running Tests

The core script for running tests is `run.py`.

- **Run all tests:**
  ```bash
  python run.py
  ```
- **Run tests in a specific directory or a single test file:**
  ```bash
  python run.py ast/
  python run.py exec/functions/parse/parse_json.tql
  ```
- **Update reference output files:**
  Use the `-u` or `--update` flag to regenerate the `.txt` or `.diff` reference
  files for failing tests or new tests.
  ```bash
  python run.py -u exec/functions/parse/parse_json.tql
  ```
- **Run tests in parallel:**
  By default, tests run in parallel. You can control the number of parallel jobs
  with the `-j` flag:
  ```bash
  python run.py -j 8
  ```

The tests are also integrated with CTest, and run by default when running the
`test` CMake build target.

## Test Structure

Tests are organized into subdirectories, each corresponding to a specific test
runner defined in `run.py`. The runner determines how the test file is executed
and what kind of output is expected.

- **Test File**: Typically a `.tql` file containing the Tenzir pipeline to test.
- **Reference File**: A file with the expected output, usually with a `.txt` or
  `.diff` extension, matching the test file's name (e.g., `my_test.tql` ->
  `my_test.txt`).

### Runner Types

- `ast/`: Tests the Abstract Syntax Tree (AST) generation (`--dump-ast`). Output
  is `.txt`.
- `exec/`: Tests end-to-end pipeline execution using the `tenzir` binary. Output
  is `.txt`.
- `node/`: Tests pipeline execution against a temporary `tenzir-node` instance.
  Output is `.txt`.
- `ir/`: Tests the optimized Intermediate Representation (IR) generation
  (`--dump-ir`). Output is `.txt`.
- `oldir/`: Tests the old optimized Intermediate Representation (IR) generation
  (`--dump-pipeline`). Output is `.txt`.
- `instantiation/`: Compares the IR before and after instantiation (`--dump-ir`
  vs `--dump-inst-ir`). Output is `.diff`.
- `opt/`: Compares the IR before and after optimization (`--dump-inst-ir` vs
  `--dump-opt-ir`). Output is `.diff`.
- `finalize/`: Tests the finalized pipeline plan (`--dump-finalized`). Output is
  `.txt`.
- `custom/`: Runs custom shell scripts (`.sh`) for more complex test scenarios.
  See the `_custom/check` script for details on how output is captured and
  compared using `.N.txt` files for steps.

## Contributing Tests

1. **Choose the Right Directory**: Place your test file (e.g., `my_feature.tql`)
   in the subdirectory corresponding to the aspect you want to test (e.g.,
   `exec/` for execution, `ast/` for syntax).
2. **Write Your Test**: Create the `.tql` file with the pipeline logic.
3. **Generate Reference Output**: Run the test script with the `--update` (`-u`)
   flag:
   ```bash
   python run.py -u path/to/your/my_feature.tql
   ```
   This will create the initial `.txt` (or `.diff`) file.
4. **Verify Output**: Carefully review the generated reference file to ensure it
   matches the expected behavior.
5. **Commit**: Add both the test file and the reference file to your commit.

## Expecting Failures

If a test is _expected_ to fail (e.g., testing error handling), start the `.tql`
file with the comment:

```tql
// error
```

The test framework will then consider the test successful only if the `tenzir`
command exits with a non-zero status code.

## Configuration Files

If a test requires specific configuration, you can place a `tenzir.yaml` file in
the same directory as the test file (e.g., `exec/my_test.tql` and
`exec/tenzir.yaml`). The test runner will automatically pass `--config
exec/tenzir.yaml` to the `tenzir` and (if applicable) `tenzir-node` processes.

## Input Files

A shared `inputs/` directory is available for test input files (e.g., sample
logs, data files). The absolute path to this directory is passed to the `tenzir`
and `tenzir-node` processes via the `INPUTS` environment variable. You can
reference files within your TQL like this:

```tql
from env("INPUTS") + "/cef/cynet.log" {
  read_cef
}
```
