setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  export TENZIR_EXEC__IMPLICIT_EVENTS_SINK='write_json | save_file "-"'
}

@test "file roundtrip" {
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | top proto"
  file="$(mktemp).parquet"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | to \"${file}\""
  check tenzir "from \"${file}\" | top proto"
}

@test "batch sizes" {
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | batch 512 | write_parquet | read_parquet | measure | drop timestamp"
}

@test "Additional write options" {
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | write_parquet compression_level=10, compression_type=\"brotli\" | read_parquet | measure | drop timestamp"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | slice begin=1150, end=1160 | write_parquet compression_level=7, compression_type=\"gzip\" | read_parquet | measure | drop timestamp"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | write_parquet compression_type=\"snappy\" | read_parquet | measure | drop timestamp"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | write_parquet compression_level=-1, compression_type=\"zstd\" | read_parquet | measure | drop timestamp"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | write_parquet compression_level=10, compression_type=\"zstd\" | read_parquet | measure | drop timestamp"
}

@test "Verify compression" {
  file1=$(mktemp)
  file2=$(mktemp)
  gunzip -c "${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz" |
    check tenzir "read_zeek_tsv | batch | to \"${file1}\" { write_parquet }"
  gunzip -c "${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz" |
    check tenzir "read_zeek_tsv | batch | to \"${file2}\" { write_parquet compression_level=7, compression_type=\"brotli\" }"
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
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | to \"${file}\" { write_parquet }"
  dd "if=${file}" "of=${file}.1k" bs=1 count=10000
  check ! tenzir "from \"${file}.1k\" { read_parquet }"
}

@test "empty records" {
  check tenzir 'from {} | write_parquet | read_parquet'
  check tenzir 'from {x: {}} | write_parquet | read_parquet'
  check tenzir 'from {x: {y: {}}} | write_parquet | read_parquet'
  check tenzir 'from {x: [{}]} | write_parquet | read_parquet'
  check tenzir 'from {x: [[{}]]} | write_parquet | read_parquet'
}
