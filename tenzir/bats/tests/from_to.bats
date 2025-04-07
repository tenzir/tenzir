: "${BATS_TEST_TIMEOUT:=120}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# -- tests --------------------------------------------------

# bats test_tags=from
@test "from deduction" {
  check tenzir --dump-pipeline "from \"example.json\""
  check tenzir --dump-pipeline "from \"file://example.json\""
  check tenzir --dump-pipeline "from \"example.json.gz\""
  check tenzir --dump-pipeline "from \"example.csv.gz\""
  check tenzir --dump-pipeline "from \"example.yaml.bz2\""
  check tenzir --dump-pipeline "from \"https://example.org/example.yaml.bz2\""
  check tenzir --dump-pipeline "from \"https://example.org/api/json\""
}

# bats test_tags=to
@test "from failure" {
  check ! tenzir --dump-pipeline "from \"unknown://example.org\""
  check ! tenzir --dump-pipeline "from \"failure.extension\""
  check ! tenzir --dump-pipeline "from \"failure.extension.bz2\""
}

# bats test_tags=to
@test "to deduction" {
  check tenzir --dump-pipeline "to \"example.json\""
  check tenzir --dump-pipeline "to \"file://example.json\""
  check tenzir --dump-pipeline "to \"example.json.gz\""
  check tenzir --dump-pipeline "to \"example.csv.gz\""
  check tenzir --dump-pipeline "to \"example.yaml.bz2\""
  check tenzir --dump-pipeline "to \"https://example.org/example.yaml.bz2\""
  check tenzir --dump-pipeline "to \"https://example.org/api/json\""
}

# bats test_tags=to
@test "to failure" {
  check ! tenzir --dump-pipeline "to \"unknown://example.org\""
  check ! tenzir --dump-pipeline "to \"failure.extension\""
  check ! tenzir --dump-pipeline "to \"failure.extension.bz2\""
}
