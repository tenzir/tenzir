setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "print one batch" {
  'version | write feather2'  
}

@test "parse one batch" {
  'version | write feather2 | read feather2'  
}

@test "parse multiple batches" {
  'version | write feather2'  
}

@test "print multiple batches" {
    'version | write feather2 | read 100 | read feather2'   
}

@test "parse and print large file" {
  'from https://storage.googleapis.com/tenzir-datasets/M57/suricata.json.zst read suricata --no-infer | where #schema == "suricata.flow" | to ./sample.feather2'
}
