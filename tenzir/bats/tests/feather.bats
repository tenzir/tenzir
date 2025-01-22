: "${BATS_TEST_TIMEOUT:=60}"

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
  if ! command -v python3; then
    skip "python3 must be in PATH"
  fi

  venv_dir=$(mktemp -d)
  python3 -m venv --system-site-packages "${venv_dir}"
  . "${venv_dir}/bin/activate"
  if ! python -c pyarrow; then
    pip install pyarrow
  fi

  tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 512 | write feather" |
    check python "${BATS_TENZIR_MISCDIR}/scripts/print-arrow-batch-size.py"
}

@test "invalid format" {
  gunzip -c "${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz" |
    check ! tenzir "read feather"
}

@test "Additional write options" {
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 256 | write feather --compression-level 10 --compression-type zstd | read feather | measure | drop timestamp"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | slice 1150:1160 | batch 512 | write feather --compression-level 7 --compression-type lz4 --min-space-savings .6 | read feather"
  gunzip -c "${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz" |
    check tenzir "read zeek-tsv | write feather --compression-level 7 | read feather | summarize count(.)"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 256 | write feather --compression-level -1 --compression-type zstd --min-space-savings 0 | read feather | measure | drop timestamp"
  gunzip -c "${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz" |
    check ! tenzir "read zeek-tsv | batch 256 | write feather --compression-level -1 --compression-type wrongname | read feather"
}

@test "Verify compression" {
  file1=$(mktemp)
  file2=$(mktemp)
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch | to ${file1} write feather --compression-type uncompressed"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch | to ${file2} write feather --compression-level 20 --compression-type zstd"
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
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 512 | to ${file} write feather"
  dd "if=${file}" "of=${file}.10k" bs=1 count=10000
  dd "if=${file}" "of=${file}.1M" bs=1 count=1000000
  check ! tenzir "from ${file}.10k read feather | summarize count(.)"
  # This test emits a warning that looks like this:
  #   warning: truncated 60584 trailing bytes
  # The exact number of trailing bytes is different between Arrow versions, so
  # we modify the warning to just say X bytes.
  tenzir "from ${file}.1M read feather | summarize count(.)" |
    check perl -pe 's/truncated \d+/truncated X/g'
}
