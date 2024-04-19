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
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 512 | write parquet | read parquet | measure | drop timestamp"
    
}

@test "invalid format" {
  check ! tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read parquet"
}

@test "Additional write options" {
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write parquet --compression-level 10 --compression-type brotli | read parquet | measure | drop timestamp"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | slice --begin 1150 --end 1160 | write parquet --compression-level 7 --compression-type gzip | read parquet"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write parquet --compression-level 7 | read parquet | summarize count(.)"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | write parquet --compression-level -1 --compression-type zstd | read parquet | measure | drop timestamp"
  check ! tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch 256 | write parquet --compression-level -1 --compression-type wrongname | read parquet"
}

@test "Verify compression" {
  file1=$(mktemp)
  file2=$(mktemp)
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch | to ${file1} write parquet"
  check tenzir "from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv | batch | to ${file2} write parquet --compression-level 7 --compression-type brotli"
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
