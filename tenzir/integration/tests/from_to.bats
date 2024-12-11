: "${BATS_TEST_TIMEOUT:=120}"

# BATS ports of our old integration test suite.

# This file contains the subset of tests that are
# executing pipelines which don't need a running
# `tenzir-node`.

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# -- tests --------------------------------------------------

# bats test_tags=from
@test "from deduction" {
  check tenzir --dump-pipeline --tql2 "from \"example.json\""
  check tenzir --dump-pipeline --tql2 "from \"file://example.json\""
  check tenzir --dump-pipeline --tql2 "from \"example.json.gz\""
  check tenzir --dump-pipeline --tql2 "from \"example.csv.gz\""
  check tenzir --dump-pipeline --tql2 "from \"example.yaml.bz2\""
  check tenzir --dump-pipeline --tql2 "from \"https://example.org/example.yaml.bz2\""
  check tenzir --dump-pipeline --tql2 "from \"https://example.org/api/json\""
}

# bats test_tags=to
@test "from failure" {
  check ! tenzir --dump-pipeline --tql2 "from \"unknown://failure\""
  check ! tenzir --dump-pipeline --tql2 "from \"failure.extension\""
  check ! tenzir --dump-pipeline --tql2 "from \"failure.extension.bz2\""
}

# bats test_tags=to
@test "to deduction" {
  check tenzir --dump-pipeline --tql2 "to \"example.json\""
  check tenzir --dump-pipeline --tql2 "to \"file://example.json\""
  check tenzir --dump-pipeline --tql2 "to \"example.json.gz\""
  check tenzir --dump-pipeline --tql2 "to \"example.csv.gz\""
  check tenzir --dump-pipeline --tql2 "to \"example.yaml.bz2\""
  check tenzir --dump-pipeline --tql2 "to \"https://example.org/example.yaml.bz2\""
  check tenzir --dump-pipeline --tql2 "to \"https://example.org/api/json\""
}

# bats test_tags=to
@test "to failure" {
  check ! tenzir --dump-pipeline --tql2 "to \"unknown://failure\""
  check ! tenzir --dump-pipeline --tql2 "to \"failure.extension\""
  check ! tenzir --dump-pipeline --tql2 "to \"failure.extension.bz2\""
}
