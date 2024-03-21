export AWS_DEFAULT_REGION="us-west-2"
export AWS_ACCESS_KEY_ID="dummy"
export AWS_SECRET_ACCESS_KEY="dummy"
export AWS_ENDPOINT_URL="http://localhost:9324"

setup() {
  command -v docker 2>/dev/null || skip "Docker is not available"
  DATADIR="${BATS_TEST_DIRNAME}/../data"
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
  container_id=$(docker run -d -p 9324:9324 \
    -e AWS_DEFAULT_REGION \
    -v "${DATADIR}/misc/elasticmq.conf:/opt/elasticmq.conf" \
    softwaremill/elasticmq-native)
  export CONTAINER_ID="${container_id}"
}

teardown() {
  command -v docker 2>/dev/null || skip "Docker is not available"
  docker kill "${CONTAINER_ID}"
}

# bats test_tags=docker
@test "send and receive messages" {
  command -v docker 2>/dev/null || skip "Docker is not available"
  check tenzir "version | put foo=42 | repeat 2 | to sqs://tenzir"
  check tenzir "from sqs://tenzir | select foo | head 2"
}
