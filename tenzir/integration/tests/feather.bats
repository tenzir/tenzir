setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "file roundtrip" {
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | top proto"
  file=$(mktemp)
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | to ${file} write feather"
  check tenzir "from ${file} read feather | top proto"
}

@test "batch sizes" {
  venv_dir=$(mktemp -d)
  python -m venv --system-site-packages "${venv_dir}"
  . "${venv_dir}/bin/activate"
  if ! python -c pyarrow; then
    pip install pyarrow
  fi

  tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 512 | write feather" |
    check python "${BATS_TENZIR_MISCDIR}/scripts/print-arrow-batch-size.py"
}

@test "invalid format" {
  check ! tenzir 'version | write json | read feather'
}

@test "truncated input" {
  file=$(mktemp)
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 512 | to ${file} write feather"
  dd "if=${file}" "of=${file}.10k" bs=1 count=10000
  dd "if=${file}" "of=${file}.1M" bs=1 count=1000000
  check ! tenzir "from ${file}.10k read feather | summarize count(.)"
  check tenzir "from ${file}.1M read feather | summarize count(.)"
}
