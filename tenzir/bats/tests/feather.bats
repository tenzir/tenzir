: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "file roundtrip" {
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | top proto"
  file="$(mktemp).feather"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | to \"${file}\""
  check tenzir "from \"${file}\" | top proto"
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

  tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | batch 512 | write_feather" |
    check python "${BATS_TENZIR_MISCDIR}/scripts/print-arrow-batch-size.py"
}

@test "invalid format" {
  gunzip -c "${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz" |
    check ! tenzir "read_feather"
}

@test "Additional write options" {
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | batch 256 | write_feather compression_level=10, compression_type=\"zstd\" | read_feather | measure | drop timestamp"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | slice begin=1150, end=1160 | batch 512 | write_feather compression_level=7, compression_type=\"lz4\", min_space_savings=0.6 | read_feather"
  gunzip -c "${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz" |
    check tenzir "read_zeek_tsv | write_feather compression_level=7 | read_feather | summarize count=count()"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | batch 256 | write_feather compression_level=-1, compression_type=\"zstd\", min_space_savings=0.0 | read_feather | measure | drop timestamp"
  gunzip -c "${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz" |
    check ! tenzir "read_zeek_tsv | batch 256 | write_feather compression_level=-1, compression_type=\"wrongname\" | read_feather"
}

@test "Verify compression" {
  file1=$(mktemp)
  file2=$(mktemp)
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | batch | to \"${file1}\" { write_feather compression_type=\"uncompressed\" }"
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | batch | to \"${file2}\" { write_feather compression_level=20, compression_type=\"zstd\" }"
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
  check tenzir "from \"${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz\" { decompress_gzip | read_zeek_tsv } | batch 512 | to \"${file}\" { write_feather }"
  dd "if=${file}" "of=${file}.10k" bs=1 count=10000
  dd "if=${file}" "of=${file}.1M" bs=1 count=1000000
  check ! tenzir "from \"${file}.10k\" { read_feather } | summarize count=count()"
  # This test emits a warning that looks like this:
  #   warning: truncated 60584 trailing bytes
  # The exact number of trailing bytes is different between Arrow versions, so
  # we modify the warning to just say X bytes.
  tenzir "from \"${file}.1M\" { read_feather } | summarize count=count()" |
    check perl -pe 's/truncated \d+/truncated X/g'
}
