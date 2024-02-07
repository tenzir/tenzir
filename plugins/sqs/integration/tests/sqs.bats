export AWS_DEFAULT_REGION="us-west-2"
export AWS_ACCESS_KEY_ID="dummy"
export AWS_SECRET_ACCESS_KEY="dummy"
export AWS_ENDPOINT_URL="http://localhost:9324"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
  container_id=$(docker run -d -p 9324:9324 \
    -e AWS_DEFAULT_REGION \
    -v "${MISCDIR}/elasticmq.conf:/opt/elasticmq.conf" \
    softwaremill/elasticmq-native)
  export CONTAINER_ID="${container_id}"
}

teardown() {
  docker kill "${CONTAINER_ID}"
}

# bats test_tags=docker
@test "list queues" {
  check aws sqs list-queues
}

# bats test_tags=docker
@test "send and receive messages" {
  check tenzir "version | put foo=42 | repeat 10 | enumerate n | to sqs://tenzir"
  # FIXME: simply piping to `head 10` does not work. But it should. We
  # workaround this weirdness by doing a `head 1`, which works.
  #
  # The sqs connector stays in an empty loop because it never receives a DOWN
  # from `head`. I confirmed this by adding the following code before the main
  # while loop:
  #
  #     ctrl.self().attach_functor([] {
  #       TENZIR_WARN("...");
  #     });
  #
  # This smells like an issue with the executor.
  check tenzir "from sqs://tenzir | select foo | head 1"
}
