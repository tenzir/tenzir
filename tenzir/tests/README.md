# Tenzir Integration Tests

This directory contains a minimal integration testing framework for Tenzir
pipelines. It uses the `run.py` script to execute Tenzir Query Language (TQL)
files and compare their output against reference files.

## Running Tests

The core script for running tests is `run.py`.

- **Run all tests:**
  ```bash
  ./run.py
  ```
- **Run tests in a specific directory or a single test file:**
  ```bash
  ./run.py ast/
  ./run.py exec/functions/parse/parse_json.tql
  ```
- **Update reference output files:**
  Use the `-u` or `--update` flag to regenerate the `.txt` or `.diff` reference
  files for failing tests or new tests.
  ```bash
  ./run.py -u exec/functions/parse/parse_json.tql
  ```
- **Run tests in parallel:**
  By default, tests run in parallel. You can control the number of parallel jobs
  with the `-j` flag:
  ```bash
  ./run.py -j 8
  ```

The tests are also integrated with CTest, and run by default when running the
`test` CMake build target.

## Test Structure

Tests are executed by a specific test runner defined in `run.py`. The runner
determines how the test file is executed and what kind of output is expected.

- **Test File**: Typically a `.tql` file containing the Tenzir pipeline to test.
- **Reference File**: A file with the expected output, usually with a `.txt` or
  `.diff` extension, matching the test file's name (e.g., `my_test.tql` ->
  `my_test.txt`).

### Specifying a Test Runner

Every test must specify which runner to use with the `test` configuration
option:

```tql
// test: ast
// error: false
```

This configuration must appear at the beginning of each test file. The directory
structure no longer determines which runner is used.

### Runner Types

- **ast**: Tests the Abstract Syntax Tree (AST) generation (`--dump-ast`). Output
  is `.txt`.
- **exec**: Tests end-to-end pipeline execution using the `tenzir` binary. Output
  is `.txt`. This is the default runner.
- **ir**: Tests the optimized Intermediate Representation (IR) generation
  (`--dump-ir`). Output is `.txt`.
- **oldir**: Tests the old optimized Intermediate Representation (IR) generation
  (`--dump-pipeline`). Output is `.txt`.
- **instantiation**: Compares the IR before and after instantiation (`--dump-ir`
  vs `--dump-inst-ir`). Output is `.diff`.
- **opt**: Compares the IR before and after optimization (`--dump-inst-ir` vs
  `--dump-opt-ir`). Output is `.diff`.
- **finalize**: Tests the finalized pipeline plan (`--dump-finalized`). Output is
  `.txt`.

## Contributing Tests

1. **Create Your Test File**: Create a `.tql` file (e.g., `my_feature.tql`) with
   your pipeline logic.
2. **Specify the Test Runner**: Add a test configuration comment at the
   beginning of your file:
   ```tql
   // test: ast
   ```
3. **Generate Reference Output**: Run the test script with the `--update` (`-u`)
   flag:
   ```bash
   ./run.py -u path/to/your/my_feature.tql
   ```
   This will create the initial `.txt` (or `.diff`) file.
4. **Verify Output**: Carefully review the generated reference file to ensure it
   matches the expected behavior.
5. **Commit**: Add both the test file and the reference file to your commit.

## Test Configuration

Tests can include configuration options in the form of specially formatted
comments at the beginning of the file. These options control how the test is
executed.

### Configuration Format

Configuration options are specified as key-value pairs in comments at the
beginning of the test file. Each configuration line must follow the format `//
key: value`:

```tql
// error: true
// timeout: 10
```

Use an empty line to separate comments at the start of the test from the
frontmatter:

```tql
// error: true

// The following test checks that:
// - Validation works correctly
// - Errors are handled properly
```

### Supported Options

- **test**: String (default: `exec`) - Specifies which test runner to use. Valid
  values: `ast`, `exec`, `ir`, `oldir`, `instantiation`, `opt`, `finalize`.
- **node**: Boolean (default: `false`) - If `true`, the test will be executed
  against a temporary `tenzir-node` instance. This works with all runner types.
- **error**: Boolean (default: `false`) - If `true`, the test is expected to
  fail (exit with non-zero status). Must be either `true` or `false`.
- **timeout**: Integer (default: `10`) - Timeout in seconds for test execution.
  Must be a positive integer.
- **skip**: String (optional) - If present, the test will be skipped and not
  executed. The value should be a string describing the reason for skipping.
  Skipped tests are reported as such in the summary and are always considered
  successful. The skip reason is printed when the test is encountered.

The test will fail immediately if:

- An unknown configuration key is used
- A configuration value has an invalid format or is out of range
- The specified test runner doesn't exist

## Skipping Tests

If you want to skip a test (for example, if it is temporarily broken or not
applicable for a certain environment), add the following to your test file's
configuration:

```tql
// skip: reason for skipping this test
```

The value should be a short string explaining why the test is skipped. When a
test is skipped, it is not executed, its output is treated as empty, and it is
always considered successful. The skip reason will be printed during test
execution, and skipped tests are counted and reported separately in the summary.

## Node Option

If you want to run a test against a temporary `tenzir-node` instance, add the
following to your test file's configuration:

```tql
// node: true
```

This can be combined with any supported runner type (e.g., `ast`, `exec`, `ir`,
etc.). When `node: true` is set, the test will be executed against a temporary
`tenzir-node` instance, and the test runner will connect to it using a
dynamically allocated endpoint.

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

## Code Coverage Integration

The test runner supports collecting code coverage information when running tests
through the `--coverage`. This helps identify which parts of the codebase are
exercised by the tests and which parts need more testing.

### Prerequisites

To use code coverage:

1. Tenzir must be built with code coverage instrumentation enabled:

   ```bash
   cmake -DTENZIR_ENABLE_CODE_COVERAGE=ON -DCMAKE_BUILD_TYPE=Debug
   ```

2. The compiler being used must support code coverage (GCC or Clang)

### Generate a Coverage Report

Use the CMake targets to process the coverage data:

```bash
# Run integration tests with coverage enabled and create a report
cmake --build path/to/build --target integration-coverage
```

The target `integration-coverage` target will run the tests and produce a
concise output that only shows the path to the HTML report. Configure the CMake
build with `CMAKE_VERBOSE_MAKEFILE=ON` to additionally display detailed
information about the coverage process, including warnings and statistics.

When running tests with coverage enabled, timeouts are automatically increased
by a factor of 5 to account for the additional overhead of code coverage
instrumentation. This helps prevent tests from failing due to the added
execution time needed for collecting coverage data.

The test runner will collect coverage data into the `ccov` directory in the
build directory (or the directory specified by the
`CMAKE_COVERAGE_OUTPUT_DIRECTORY` environment variable).

### Processing Coverage Results

## Future Work

- **Add mechanism to run arbitrary code (e.g., a Docker container) while the
  test is running:**
  Enable tests to launch and interact with external processes or services (such
  as Docker containers, mock servers, or custom scripts) during execution. This
  would allow for more comprehensive integration testing, including scenarios
  that require external dependencies or simulate real-world environments.

- **Add mechanism for skipping tests if certain Tenzir plugins are not available
  in the `tenzir` binary:**
  Some tests may require specific plugins to be present in the Tenzir binary.
  Introduce a way to declare required plugins in the test frontmatter and
  automatically skip tests (with a clear reason) if those plugins are not
  detected, improving developer experience and CI reliability.

- **Port the remaining tests over from the old test framework:**
  There are still tests in the legacy framework that have not yet been migrated.
  These should be reviewed, updated as necessary, and ported to this new
  framework to ensure full coverage and consistency.

- **Allow tests to be contributed in a separate repository as well so that
  larger inputs can be used for testing without blowing up this repository's
  size:**
  To support large-scale or data-heavy tests without bloating the main
  repository, add support for referencing or importing tests (and their input
  data) from external repositories. This would enable more extensive testing
  scenarios and facilitate community contributions of large or specialized test
  suites.

- **Integrate the testing framework with the example plugin repository for
  third-party developer tests:**
  Provide a mechanism for third-party plugin developers to write and run their
  own integration tests using this framework, ideally by integrating with the
  example plugin repository. This will help ensure plugin compatibility and
  correctness, and foster a healthy ecosystem of community-contributed plugins
  with robust test coverage.

- **Further enhance code coverage support:**
  Improve the current code coverage integration by:

  - Integrating with CI/CD pipelines
  - Adding summary statistics and coverage badges
  - Implementing coverage trend tracking over time

- **Integrate the test framework with Tenzir packages:**
  Develop a mechanism for installing Tenzir packages and running
  package-provided tests in an isolated environment where the package is
  available. This will allow package authors to ship their own integration tests
  and ensure that their package works correctly when installed alongside the
  Tenzir binary.
