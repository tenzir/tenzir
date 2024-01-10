# The Tenzir Integration Testing Framework

We run our integration tests on top of [`bats`](https://bats-core.readthedocs.io).

There is one custom library `bats-tenzir` that provides
common node setup and a `check` function that compares
the test output against a known reference file.

# Running integration tests

To run all tests, execute the `integration` target from cmake.

To run all tests in this directory, use `bats tests/`.

To run all tests in a specific test file, use e.g. `bats tests/version.test`

To run the set of tests that failed during the last run of bats,
the `--filter-status failed` option can be used. This can also
be useful in combination with the `UPDATE` option below when updating
tests after implementing a breaking change.

To run one specific test, add the `bats:focus` tag to that test and
run the test file.

# Updating and creating references

To update or create reference files, set the `UPDATE=1` environment
variable and run `bats`.

To update the references for one specific test, add the `bats:focus`
tag to that test and run bats with `UPDATE=1`. Only that test will
be executed and updated.

# Temporary files

There are variables `$BATS_TEST_TMPDIR` and `$BATS_FILE_TMPDIR`
automatically defined that point to a temporary directory that
is unique for this test or unique for the current test file
respectively.
