setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "file roundtrip" {
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | top proto"
  file=$(mktemp)
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | to ${file} write parquet"
  check tenzir "from ${file} read parquet | top proto"
}

@test "batch sizes" {
  if ! command -v python3; then
    skip "python3 must be in PATH"
  fi

  venv_dir=$(mktemp -d)
  python3 -m venv --system-site-packages "${venv_dir}"
  . "${venv_dir}/bin/activate"
  if ! python -c pyarrow; then
    pip install pyarrow
  fi

  tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 512 | write parquet" |
    check python "${BATS_TENZIR_MISCDIR}/scripts/print-arrow-batch-size.py"
}


@test "invalid format" {
  check ! tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read parquet
}

@test "Additional write options" {
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write parquet --compression-level 10 --compression-type zstd | read parquet"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write parquet --compression-level -1 --compression-type lz4 | read parquet"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write parquet --compression-level -1 --compression-type zstd | read parquet"
  check ! tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 256 | write parquet --compression-level -1 --compression-type wrongname | read parquet"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/suricata/eve.json read suricata --no-infer | where #schema == \"suricata.flow\" | write parquet --compression-type uncompressed | read parquet"
}

@test "Verify compression" {
  file1=$(mktemp)
  file2=$(mktemp)
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch | to ${file1} write parquet"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch | to ${file2} write parquet --compression-level 20 --compression-type zstd"
  filesize() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
      stat -f %z "$@"
    else
      stat -c %s "$@"
    fi
  }
  size1=$(filesize "$file1")
  size2=$(filesize "$file2")
  check [ "$size1" -gt "$size2" ]
}

@test "truncated input" {
  file=$(mktemp)
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 512 | to ${file} write parquet"
  dd "if=${file}" "of=${file}.10k" bs=1 count=10000
  dd "if=${file}" "of=${file}.1M" bs=1 count=1000000
  check ! tenzir "from ${file}.10k read parquet | summarize count(.)"
  # This test emits a warning that looks like this:
  #   warning: truncated 60584 trailing bytes
  # The exact number of trailing bytes is different between Arrow versions, so
  # we modify the warning to just say X bytes.
  tenzir "from ${file}.1M read parquet | summarize count(.)" |
    check perl -pe 's/truncated \d+/truncated X/g'
}
