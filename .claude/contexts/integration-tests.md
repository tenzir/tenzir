**Test Framework Architecture:**

The Tenzir integration testing framework uses `run.py` to execute TQL files and compare their output against reference files. Tests are organized by functionality in subdirectories.

**Setup:**

- All instructions assume that you are in the `tenzir/tests/` directory
- The test runner uses `typing.override` which is only available in Python 3.12+.
- Specify Python 3.12+ when using `uv`:

  ```bash
  uv run --python 3.12 run.py   # Correct
  uv run run.py                 # WRONG - may use wrong Python version
  ```

**Test Runner Types:**

Every test MUST specify a runner using the `test` configuration option at the beginning of the file:

- **exec** (default): End-to-end pipeline execution, produces .txt output
- **ast**: Tests Abstract Syntax Tree generation, produces .txt output
- **ir**: Tests optimized Intermediate Representation, produces .txt output
- **oldir**: Tests old optimized IR, produces .txt output
- **instantiation**: Compares IR before/after instantiation, produces .diff output
- **opt**: Compares IR before/after optimization, produces .diff output
- **finalize**: Tests finalized pipeline plan, produces .txt output

**Test Configuration Format:**

Tests use specially formatted comments at the beginning for configuration:

```tql
// test: exec
// error: false
// timeout: 10
// node: false
// skip: reason for skipping (optional)
```

Configuration options:

- **test**: Runner type (default: exec)
- **node**: Run against temporary tenzir-node instance (default: false)
- **error**: Test expects failure/non-zero exit (default: false)
- **timeout**: Execution timeout in seconds (default: 10)
- **skip**: Skip test with given reason (optional)

Separate configuration from regular comments with an empty line.

**Core Responsibilities:**

1. **Test Creation**:

   - Analyze the feature to determine comprehensive test scenarios
   - Create .tql files with the appropriate runner configuration
   - Cover typical usage, edge cases, and error conditions
   - Place tests in appropriate subdirectories (exec/, ast/, etc.)
   - Use `uv run run.py -u` to generate initial reference files (.txt or .diff)
   - Never manually create reference output files

2. **Test Execution**:

   - Execute tests via `uv run run.py`
   - Run all tests: `uv run run.py`
   - Run specific tests: `uv run run.py exec/functions/parse.tql`
   - Run directory: `uv run run.py exec/functions/`
   - Use `-j N` for parallel execution (default: parallel)
   - Tests are also integrated with CTest

3. **Baseline Management**:
   - Update reference files with `uv run run.py -u exec/operators/test.tql`
   - Verify updates reflect intended changes, not regressions
   - Review generated reference files carefully before committing

**Test Environment Features:**

1. **Configuration Files**:

   - Place `tenzir.yaml` in test directory for specific configuration
   - Automatically passed via `--config` to tenzir/tenzir-node processes

2. **Input Files**:

   - Shared `inputs/` directory for test data
   - Access via `INPUTS` environment variable in TQL:

   ```tql
   from env("INPUTS") + "/cef/cynet.log" {
     read_cef
   }
   ```

3. **Node Testing**:
   - Add `// node: true` to test against temporary tenzir-node
   - Works with all runner types
   - Uses dynamically allocated endpoint

**Code Coverage Integration:**

When Tenzir is built with coverage enabled:

- Run with coverage: `cmake --build build --target integration-coverage`
- Timeouts automatically increased 5x for coverage overhead
- Coverage data collected in `ccov` directory

**Test Writing Guidelines:**

1. **Structure**:

   - One test per file for specific functionality
   - Use descriptive filenames indicating what's tested
   - Group related tests in subdirectories
   - Include explanatory comments for complex scenarios

2. **Configuration**:

   - Always specify runner type explicitly
   - Set appropriate timeout for complex tests
   - Use `error: true` for tests expecting failure
   - Use `skip:` with reason for temporarily disabled tests

3. **Best Practices**:

   - Make tests deterministic and reproducible
   - Avoid external dependencies or timing-sensitive operations
   - Use `sort` operator for non-deterministic output
   - Keep tests self-contained with clear inputs/outputs
   - Test both success paths and error handling

4. **Error Testing**:
   - When testing error conditions, set `error: true`
   - Verify error messages are appropriate and helpful
   - Test invalid inputs and edge cases

**Workflow Patterns:**

1. **New Feature Testing**:

   - Read feature documentation thoroughly
   - Design test cases covering all aspects
   - Create .tql with appropriate runner configuration
   - Generate baselines with `uv run run.py -u`
   - Verify output correctness

2. **Regression Testing**:

   - Identify affected areas from code changes
   - Run targeted test suites
   - Investigate failures before updating baselines
   - Only update if changes are intentional

3. **Test Debugging**:
   - Run single test for focused debugging
   - Check test configuration is correct
   - Verify input files exist if used
   - Compare actual vs expected output

**Directory Organization:**

- Follow existing taxonomy (operators/, functions/, formats/)
- Create subdirectories for new functionality categories
- Keep related tests together
- Use consistent naming patterns
