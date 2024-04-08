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
  tmp_file = $(mktemp)
  ${tmp_file} 
  check ! tenzir 'from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write json | read feather'
}

@test "Additional write options" {
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/suricata/eve.json | write feather --compression-type uncompressed | read feather"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write feather --compression-level 10 --compression-type zstd --min-space-savings .6"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write feather --compression-level -1 --compression-type lz4 --min-space-savings 1"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write feather --compression-level -1 --compression-type zstd --min-space-savings 0"
  check ! tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 256 | write feather --compression-level -1 --compression-type zstd --min-space-savings 10"
}

@test "Verify compression" {
    file1=$(mktemp)
    file2=$(mktemp)
    check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/suricata/eve.json | batch 256 | to ${file1} write feather --compression-type uncompressed"
    check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/suricata/eve.json | batch 256 | to ${file2} write feather --compression-level -1 --compression-type lz4 --min-space-savings 1"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        size1=$(stat -f%z "$file1")
        size2=$(stat -f%z "$file2")
        check [ "$size1" -gt "$size2" ]
    else
        # Linux
        size1=$(stat -c %s "$file1")
        size2=$(stat -c %s "$file2")
        check [ "$size1" -gt "$size2" ]
    fi
}



@test "truncated input" {
  file=$(mktemp)
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 512 | to ${file} write feather"
  dd "if=${file}" "of=${file}.10k" bs=1 count=10000
  dd "if=${file}" "of=${file}.1M" bs=1 count=1000000
  check ! tenzir "from ${file}.10k read feather | summarize count(.)"
  check tenzir "from ${file}.1M read feather | summarize count(.)"
}
