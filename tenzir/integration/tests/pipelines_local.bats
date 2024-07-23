: "${BATS_TEST_TIMEOUT:=120}"

# BATS ports of our old integration test suite.

# This file contains the subset of tests that are
# executing pipelines which don't need a running
# `tenzir-node`.

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# -- tests --------------------------------------------------

# bats test_tags=parser
@test "Parse basic" {
  tenzir --dump-ast " "
  tenzir --dump-ast "// comment"
  tenzir --dump-ast "#!/usr/bin/env tenzir"
}

# bats test_tags=parser
@test "Parse operators" {
  check tenzir --dump-ast "version"
  check ! tenzir "version --tev"
  check ! tenzir "version 42"
  check ! tenzir "from a/b/c.json read json"
  check tenzir --dump-ast "from file a/b/c.json"
  check tenzir --dump-ast "from file a/b/c.json read cef"
  check tenzir --dump-ast "read zeek-tsv"
  check tenzir --dump-ast "head 42"
  check tenzir --dump-ast "local remote local pass"
  check tenzir --dump-ast "where :ip == 1.2.3.4"
  check tenzir --dump-ast "to file xyz.json"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json"
  check tenzir "from file://${INPUTSDIR}/json/all-types.json read json"
  check ! tenzir "from file:///foo.json read json"
  check ! tenzir "from scheme://foo.json read json"
  check ! tenzir "from scheme:foo.json read json"
  check ! tenzir "load file foo.json | read json"
  check ! tenzir "load file | read json"
  check ! tenzir "load ./file | read json"
  check ! tenzir "load filee | read json"
  check tenzir "from ${INPUTSDIR}/json/basic-types.json"
  check tenzir "from ${INPUTSDIR}/json/dns.log.json.gz | head 2"
  check tenzir "from ${INPUTSDIR}/json/dns.log.json.gz read json | head 2"
  check tenzir "from ${INPUTSDIR}/suricata/eve.json | head 2"
  check tenzir "from ${INPUTSDIR}/zeek/http.log.gz read zeek-tsv | head 2"
  check tenzir --dump-ast "from a/b/c.json | where xyz == 123 | to foo.csv"
}

# bats test_tags=pipelines
@test "Apply operator" {
  check tenzir "apply ${QUERYDIR}/some_source | write json"
  check tenzir "apply ${QUERYDIR}/some_source.tql | write json"
  check ! tenzir "apply /tmp/does_not_exist"
  check ! tenzir "apply does_not_exist.tql"
  run ! tenzir "apply ${QUERYDIR}/from_unknown_file.tql"
}

# bats test_tags=pipelines
@test "Local Pipeline Execution" {
  # - is an alternative form of stdin and stdout
  check -c "gunzip -c '${INPUTSDIR}'/json/sip.log.json.gz | tenzir 'from stdin read json | write json | save stdout'"
  check -c "gunzip -c '${INPUTSDIR}'/json/sip.log.json.gz | tenzir 'from file - read json | to stdout write json'"

  # stdin and stdout are the defaults
  check -c "gunzip -c '${INPUTSDIR}'/json/files.log.json.gz | tenzir 'read json | write json'"

  # - is an alternative form of stdin and stdout
  check -c "gunzip -c '${INPUTSDIR}'/json/irc.log.json.gz | tenzir 'from - read json | to - write json '"

  check -c "gunzip -c '${INPUTSDIR}'/json/irc.log.json.gz | tenzir 'from file - read json | to - write json'"
}

# bats test_tags=pipelines
@test "Read from JSON File" {
  check tenzir "from file ${INPUTSDIR}/json/record-in-list.json read json | write json"
}

# bats test_tags=json
@test "Read incomplete JSON object" {
  check ! tenzir "from file ${INPUTSDIR}/json/incomplete-object.json"
}

# bats test_tags=pipelines
@test "Type mismatch in a column" {
  check tenzir "from file ${INPUTSDIR}/json/type-mismatch.json read json | write json"
}

# bats test_tags=pipelines
@test "Use schema time unit when converting from a double to a duration" {
  check tenzir "from file ${INPUTSDIR}/json/double-to-duration-cast.json read json --selector=schema:argus | select SIntPkt | write json"
}

# bats test_tags=pipelines,json
@test "Read JSON with nested selector field" {
  check tenzir "from file ${INPUTSDIR}/suricata/eve.json read json --selector=flow.start | put x=#schema"
}

# bats test_tags=pipelines,json
@test "Read JSON with integer selector" {
  check tenzir "from file ${INPUTSDIR}/suricata/eve.json read json --selector=pcap_cnt | put x=#schema"
}

# bats test_tags=pipelines
@test "Read from suricata file" {
  check tenzir "from file ${INPUTSDIR}/suricata/eve.json read suricata | write json"
  check tenzir "from file ${INPUTSDIR}/suricata/eve.json read json --schema=suricata.alert --no-infer | write json"
}

# bats test_tags=pipelines
@test "Skip columns that are not in the schema for suricata input with no-infer option" {
  check tenzir "from file ${INPUTSDIR}/suricata/dns-with-no-schema-column.json read suricata --no-infer | select custom_field | write json"
}

# bats test_tags=pipelines
@test "Read from zeek json file" {
  check tenzir "from file ${INPUTSDIR}/zeek/zeek.json read zeek-json | write json"
}

# bats test_tags=json
@test "Read JSON from tshark output" {
  check tenzir "from file ${INPUTSDIR}/pcap/tshark.json"
}

# bats test_tags=json
@test "Read JSON with new field in record list" {
  check tenzir "from file ${INPUTSDIR}/json/record-list-new-field.json"
}

# bats test_tags=json
@test "Read JSON with differents fields in one record list" {
  check tenzir "from file ${INPUTSDIR}/json/record-list-different-fields.json"
}

# bats test_tags=json
@test "Read JSON with list config in overwritten field" {
  check tenzir "from file ${INPUTSDIR}/json/record-list-conflict-field-overwrite.json"
}

# bats test_tags=json
@test "Read JSON record list with nulls and conflict" {
  check tenzir "from file ${INPUTSDIR}/json/record-list-with-null-conflict.json"
}

# bats test_tags=pipelines, cef
@test "Schema ID Extractor" {
  check -c "cat ${INPUTSDIR}/cef/forcepoint.log | tenzir 'read cef | put fingerprint = #schema_id | write json'"

  check -c "cat ${INPUTSDIR}/cef/forcepoint.log | tenzir 'read cef | where #schema_id == \"6aeddcaa9adee9b9\" | write json'"

  check -c "cat ${INPUTSDIR}/cef/forcepoint.log | tenzir 'read cef | where #schema_id != \"6aeddcaa9adee9b9\" | write json'"
}

# bats test_tags=pipelines
@test "Measure Events" {
  check tenzir "from ${INPUTSDIR}/json/files.log.json.gz read json | measure | summarize events=sum(events) by schema | write json"
  check tenzir "from ${INPUTSDIR}/json/files.log.json.gz read json | measure --real-time | summarize events=sum(events) by schema | write json"
}

# bats test_tags=pipelines
@test "Measure Bytes" {
  check tenzir "from ${INPUTSDIR}/json/conn.log.json.gz | measure | summarize bytes=sum(bytes) | write json"
  check tenzir "from ${INPUTSDIR}/json/conn.log.json.gz | measure --real-time | summarize bytes=sum(bytes) | write json"
}

# bats test_tags=pipelines
@test "Batch Events" {
  check tenzir 'version | repeat 10 | batch 5 | measure | select events'
  check tenzir 'version | repeat 10 | batch 1 | measure | select events'
  check tenzir 'version | repeat 10 | batch 3 | measure | select events'
  check tenzir 'version | repeat 10 | batch 15 | measure | select events'
}
# bats test_tags=pipelines
@test "Empty Record in Pipeline" {
  check tenzir "from ${INPUTSDIR}/json/empty-record.json read json | write json"
  check tenzir "from ${INPUTSDIR}/json/empty-record.json read json | write csv"
  check tenzir "from ${INPUTSDIR}/json/empty-record.json read json | write xsv \" \" ; NULL"
}

# bats test_tags=pipelines, repeat
@test "Repeat" {
  check tenzir "load ${INPUTSDIR}/cef/forcepoint.log | read cef | write json"
  check tenzir "load ${INPUTSDIR}/cef/forcepoint.log | repeat 5 | read cef | write json"
  check tenzir "load ${INPUTSDIR}/cef/forcepoint.log | read cef | repeat 5 | write json"
  check tenzir "load ${INPUTSDIR}/cef/forcepoint.log | read cef | measure | summarize sum(events) by schema | write json"
  check tenzir "load ${INPUTSDIR}/cef/forcepoint.log | repeat 5 | read cef | measure | summarize sum(events) by schema | write json"
  check tenzir "load ${INPUTSDIR}/cef/forcepoint.log | read cef | repeat 5 | measure | summarize sum(events) by schema | write json"
}

# bats test_tags=pipelines
@test "Heterogeneous Lists" {
  cat ${INPUTSDIR}/json/basic-types.json |
    check ! tenzir "from stdin read json | put foo=[\"true\", false]"

  cat ${INPUTSDIR}/json/basic-types.json |
    check ! tenzir "from stdin read json | extend foo=[\"true\", false]"

  cat ${INPUTSDIR}/json/basic-types.json |
    check ! tenzir "from stdin read json | replace d=[\"true\", false]"

  cat ${INPUTSDIR}/json/basic-types.json |
    check ! tenzir "from stdin read json | put foo=[[\"true\"], false]"

  cat ${INPUTSDIR}/json/basic-types.json |
    check ! tenzir "from stdin read json | put foo=[[\"true\"], [false]]"

  cat ${INPUTSDIR}/json/basic-types.json |
    check tenzir "from stdin read json | put foo=[[\"true\"], [\"false\"]]"
}

# bats test_tags=pipelines,zeek
@test "Zeek TSV Pipeline Format" {
  check tenzir "from ${INPUTSDIR}/zeek/merge.log read zeek-tsv | write json"
  check tenzir "from ${INPUTSDIR}/zeek/merge_with_whitespace_separation.log read zeek-tsv | write json"
  check tenzir "from ${INPUTSDIR}/zeek/dns.log.gz read zeek-tsv | head 300 | write zeek-tsv --disable-timestamp-tags"
  check tenzir "from ${INPUTSDIR}/zeek/dns.log.gz read zeek-tsv | head 300 | batch | write csv"
  check tenzir "from ${INPUTSDIR}/zeek/whitespace_start.log read zeek-tsv | write zeek-tsv --disable-timestamp-tags --set-separator \";\" --empty-field \"empty\" --unset-field \"NULLVAL\""
  check tenzir "from ${INPUTSDIR}/json/snmp.log.json.gz read json | write zeek-tsv --disable-timestamp-tags"
  check tenzir "from ${INPUTSDIR}/zeek/empty.log read zeek-tsv | write zeek-tsv --disable-timestamp-tags"
  check tenzir "from ${INPUTSDIR}/zeek/broken_no_separator_header.log read zeek-tsv | write zeek-tsv --disable-timestamp-tags"
  check tenzir "from ${INPUTSDIR}/zeek/broken_no_set_separator_header.log read zeek-tsv | write zeek-tsv --disable-timestamp-tags"
  check tenzir "from ${INPUTSDIR}/zeek/broken_no_separator_value.log read zeek-tsv | write zeek-tsv --disable-timestamp-tags"
  check tenzir "from ${INPUTSDIR}/zeek/broken_no_empty_and_unset_fields.log read zeek-tsv | write zeek-tsv --disable-timestamp-tags"
  check tenzir "from ${INPUTSDIR}/zeek/broken_no_closing_tag.log read zeek-tsv | write zeek-tsv --disable-timestamp-tags"
  check tenzir "from ${INPUTSDIR}/zeek/broken_no_data_after_open.log read zeek-tsv | write zeek-tsv --disable-timestamp-tags"

  cat ${INPUTSDIR}/zeek/broken_unequal_fields_types_length.log |
    check ! tenzir "from stdin read zeek-tsv | write zeek-tsv --disable-timestamp-tags"
  cat ${INPUTSDIR}/zeek/broken_duplicate_close_tag.log |
    check ! tenzir "from stdin read zeek-tsv | write ! zeek-tsv --disable-timestamp-tags"
  cat ${INPUTSDIR}/zeek/broken_data_after_close_tag.log |
    check ! tenzir "from stdin read zeek-tsv | write ! zeek-tsv --disable-timestamp-tags"
  cat ${INPUTSDIR}/zeek/duplicate_field_name.log |
    check ! tenzir "from stdin read zeek-tsv | write zeek !-tsv --disable-timestamp-tags"
}

# bats test_tags=pipelines, zeek
@test "Sort" {
  check tenzir "from ${INPUTSDIR}/zeek/merge.log read zeek-tsv | select ts, uid | sort ts | write json"
  check tenzir "from ${INPUTSDIR}/zeek/merge.log read zeek-tsv | select uid | sort uid desc | write json"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head | select service | sort service | write json"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head | select service | sort service nulls-first | write json"
  check tenzir "from ${INPUTSDIR}/pcap/zeek/conn.log.gz read zeek-tsv | select id | sort id.orig_h, id.orig_p, id.resp_h, id.resp_p | head 100 | write lines"
}

# bats test_tags=pipelines
@test "Slice Regression Test" {
  # This tests for a bug fixed by tenzir/tenzir#3171 that caused sliced nested
  # arrays to be accessed incorrectly, resulting in a crash. The head 8 and tail
  # 3 operators are intentionally chosen to slice in the middle of a batch.
  check tenzir "from ${INPUTSDIR}/cef/forcepoint.log read cef | select extension.dvc | head 8 | extend foo=extension.dvc | write json"
  check tenzir "from ${INPUTSDIR}/cef/forcepoint.log read cef | select extension.dvc | tail 3 | extend foo=extension.dvc | write json"
}

# bats test_tags=pipelines, zeek
@test "Shell" {
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 1 | write json -c | shell rev"
  check tenzir 'shell "echo foo"'
  check tenzir 'shell "{ echo \"#\"; seq 1 2 10; }" | read csv | write json -c'
}

# bats test_tags=pipelines
@test "Summarize All None Some" {
  # The summarize operator supports using fields which do not exist, using
  # `null` instead of their value. Here, we test many combinations of this
  # behavior. We use the letters A, N and S for all, none and some,
  # respectively. For example, SA means that the some (but not all)
  # schemas do not have the aggregation column present, but for all
  # of them, the group-by column exists.

  # AA
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(_path) by _path"
  # NN
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(y) by z"
  # NA
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(y) by _path"
  # AN
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(_path) by z"
  # NS
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(y) by id.orig_h"
  # SN
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(id.orig_h) by z"
  # AS
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(_path) by id.orig_h"
  # SA
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(id.orig_h) by _path"
  # SS
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(id.orig_h) by id.orig_h"
  # A
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(_path)"
  # S
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(id.orig_h)"
  # N
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=distinct(y)"
}

# bats test_tags=pipelines
@test "Summarize Dot" {
  check tenzir "from ${INPUTSDIR}/zeek/zeek.json read zeek-json | summarize x=count(.)"
  cat ${INPUTSDIR}/zeek/zeek.json |
    check ! tenzir "from stdin read zeek-json | summarize x=distinct(.)"
  cat ${INPUTSDIR}/zeek/zeek.json |
    check ! tenzir "from stdin read zeek-json | summarize x=count(_path) by ."
}

# bats test_tags=pipelines, zeek
@test "Flatten Operator" {
  check tenzir "from ${INPUTSDIR}/zeek/dns.log.gz read zeek-tsv | tail 10 | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/nested-object.json read json | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/nested-structure.json read json | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-in-list.json read json | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/suricata/eve.json read suricata | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | flatten | to stdout"

  # TODO: Reenable tests with only record flattening.
  # check tenzir "from ${INPUTSDIR}/zeek/dns.log.gz read zeek-tsv | tail 10 | flatten -l | to stdout"
  # check tenzir "from ${INPUTSDIR}/json/nested-structure.json read json | flatten -l | to stdout"
  # check tenzir "from ${INPUTSDIR}/json/record-in-list.json read json | flatten -l | to stdout"
}

# bats test_tags=pipelines
@test "Unflatten Operator" {
  check tenzir "from ${INPUTSDIR}/json/record-in-list-in-record.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/records-in-nested-lists.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/records-in-nested-record-lists.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-in-list.json read json | flatten | unflatten | write json"
  check tenzir "from ${INPUTSDIR}/json/nested-object.json read json | flatten | unflatten | write json"
  check tenzir "from ${INPUTSDIR}/json/nested-structure.json read json | flatten | unflatten | write json"
  check tenzir "from ${INPUTSDIR}/zeek/dns.log.gz read zeek-tsv | tail 10 | flatten | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-in-list2.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-with-multiple-unflattened-values.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-with-multi-nested-field-names.json read json | unflatten | to stdout"
  check tenzir "unflatten" <<EOF
{}
EOF
  # {x.y: int64, x: {}}
  check tenzir "unflatten" <<EOF
{"x.y": 1, "x": {}}
{"x.y": null, "x": {}}
{"x.y": 1, "x": null}
{"x.y": null, "x": null}
EOF
  # {x.y: int64, x: {z: int64}}
  check tenzir "unflatten" <<EOF
{"x.y": 1, "x": {"z": 2}}
{"x.y": null, "x": {"z": 2}}
{"x.y": 1, "x": {"z": null}}
{"x.y": 1, "x": null}
{"x.y": null, "x": null}
EOF
  # {x.y: {z: int64}, x: {y.z: int64}}
  check tenzir "unflatten" <<EOF
{"x.y": {"z": 1}, "x": {"y.z": 2}}
{"x.y": null, "x": {"y.z": 2}}
{"x.y": {"z": 1}, "x": null}
{"x.y": null, "x": null}
EOF
  # {x.y: {z: {a.b: int64}}, x: {y.z: {a.c: int64}}}
  check tenzir "unflatten" <<EOF
{"x.y": {"z": {"a.b": 1}}, "x": {"y.z": {"a.c": 2}}}
{"x.y": {"z": null}, "x": {"y.z": {"a.c": 2}}}
{"x.y": {"z": {"a.b": 1}}, "x": {"y.z": null}}
{"x.y": {"z": null}, "x": {"y.z": null}}
{"x.y": {"z": {"a.b": 1}}, "x": null}
{"x.y": null, "x": {"y.z": {"a.c": 2}}}
{"x.y": null, "x": null}
EOF
}

# bats test_tags=pipelines
@test "JSON Printer" {
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --compact-output"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --omit-nulls"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --omit-empty-objects"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --omit-empty-lists"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --omit-empty"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | flatten | write json"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | flatten | write json --omit-empty"
}

# bats test_tags=pipelines, parser, printer, pcap
@test "PCAP Format" {
  # Make sure basic decapsulation logic works.
  check tenzir "from ${INPUTSDIR}/pcap/example.pcap.gz read pcap | decapsulate | drop pcap.data | write json"
  # Decapsulate VLAN information. Manually verified with:
  # tshark -r vlan-single-tagging.pcap -T fields -e vlan.id
  check tenzir "from ${INPUTSDIR}/pcap/vlan-single-tagging.pcap read pcap | decapsulate | select vlan.outer, vlan.inner | write json"
  check tenzir "from ${INPUTSDIR}/pcap/vlan-double-tagging.pcap read pcap | decapsulate | select vlan.outer, vlan.inner | write json"
  # Re-produce an identical copy of the input by taking the input PCAP file
  # header as blueprint for the output trace. The MD5 of the original input
  # is 2696858410a08f5edb405b8630a9858c.
  check -c "tenzir 'from ${INPUTSDIR}/pcap/example.pcap.gz read pcap -e | write pcap' | md5sum | cut -f 1 -d ' '"
  # Concatenate PCAPs and process them. The test ensures that we have the
  # right sequencing of file header and packet header events.
  check tenzir "shell \"cat ${INPUTSDIR}/pcap/vlan-*.pcap\" | read pcap -e | put schema=#schema | write json -c"
}

# bats test_tags=pipelines, compression
@test "Compression" {
  # TODO: Also add tests for lz4, zstd, bz2, and brotli, and compression in
  # general. The current integration testing framework does not support
  # testing binary outputs very well, so we should implement more tests once
  # we're completed the transition to bats (see tenzir/tenzir#2859).
  check tenzir "load file ${INPUTSDIR}/json/conn.log.json.gz | decompress gzip | read zeek-json | summarize num_events=count(.)"
}

# bats test_tags=pipelines, formats
@test "Lines" {
  check tenzir "from ${INPUTSDIR}/cef/checkpoint.log read lines | summarize n=count(.) | write json -c"
  check tenzir "from ${INPUTSDIR}/cef/checkpoint.log read lines -s | summarize n=count(.) | write json -c"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | write lines"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | put e | write lines"
  check tenzir "from ${INPUTSDIR}/json/type-mismatch.json read json | write lines"
}

# bats test_#tags=pipelines
@test "S3 Connector" {
  # TODO: Set up Tenzir S3 stuff for Tenzir-internal read/write tests?
  # TODO: Re-enable this test once Arrow updated their bundled AWS SDK from version 1.10.55,
  # see: https://github.com/apache/arrow/issues/37721
  skip "Disabled due to arrow upstream issue"

  check tenzir 'from s3 s3://sentinel-cogs/sentinel-s2-l2a-cogs/1/C/CV/2023/1/S2B_1CCV_20230101_0_L2A/tileinfo_metadata.json | write json'
}

# bats test_tags=pipelines
@test "Get and Set Attributes" {
  check tenzir 'version | set-attributes --foo bar --abc=def | get-attributes'
  check tenzir 'version | set-attributes --first 123 | set-attributes --second 456 | get-attributes'
}

# bats test_tags=pipelines,chart
@test "Chart Arguments" {
  cat ${INPUTSDIR}/json/all-types.json |
    check ! tenzir "from stdin read json | chart pie -x b"
  cat ${INPUTSDIR}/json/all-types.json |
    check ! tenzir "from stdin read json | chart pie --value b -x e"
  cat ${INPUTSDIR}/json/all-types.json |
    check ! tenzir "from stdin read json | chart piett --value=b"
  cat ${INPUTSDIR}/json/all-types.json |
    check ! tenzir "from stdin read json | chart bar -x=foo,bar -y=field"
  cat ${INPUTSDIR}/json/all-types.json |
    check ! tenzir "from stdin read json | chart bar --x-axis field"

  check tenzir "from stdin read json | chart bar -x first -y=second,third" <<EOF
{"first": 1, "second": "Hello world", "third": "foo"}
{"first": 2, "second": "Hallo Welt", "third": "bar"}
{"first": 3, "second": "Hei maailma", "third": "baz"}
EOF
}

# bats test_tags=pipelines
@test "Yield Operator" {
  cat ${INPUTSDIR}/suricata/rrdata-eve.json |
    check tenzir "read suricata | yield dns"
  cat ${INPUTSDIR}/suricata/rrdata-eve.json |
    check tenzir "read suricata | yield dns.foo"
  cat ${INPUTSDIR}/suricata/rrdata-eve.json |
    check tenzir "read suricata | yield dns.answers"
  cat ${INPUTSDIR}/suricata/rrdata-eve.json |
    check tenzir "read suricata | yield dns.answers[]"
  cat ${INPUTSDIR}/suricata/rrdata-eve.json |
    check tenzir "read suricata | yield dns.answers[].ttl"
  cat ${INPUTSDIR}/suricata/rrdata-eve.json |
    check tenzir "read suricata | yield dns.answers[].soa"
}

# bats test_tags=pipelines, syslog
@test "Syslog format" {
  check tenzir "from ${INPUTSDIR}/syslog/syslog.log read syslog"
  check tenzir "from ${INPUTSDIR}/syslog/syslog-rfc3164.log read syslog"
  check tenzir "from ${INPUTSDIR}/syslog/multiline.log read syslog"
}

# bats test_tags=pipelines
@test "Parse CEF in JSON in JSON from Lines" {
  check tenzir "from ${INPUTSDIR}/json/cef-in-json-in-json.json read lines"
  check tenzir "from ${INPUTSDIR}/json/cef-in-json-in-json.json read lines | parse line json"
  check tenzir "from ${INPUTSDIR}/json/cef-in-json-in-json.json read lines | parse line json | parse line.foo json"
  check tenzir "from ${INPUTSDIR}/json/cef-in-json-in-json.json read lines | parse line json | parse line.foo json | parse line.foo.baz cef"
}

# bats test_tags=pipelines
@test "Parse CEF over syslog" {
  check tenzir "from ${INPUTSDIR}/syslog/cef-over-syslog.log read syslog"
  check tenzir "from ${INPUTSDIR}/syslog/cef-over-syslog.log read syslog | parse content cef"
}

# bats test_tags=pipelines
@test "Parse with Grok" {
  check tenzir 'show version | put version="v4.5.0-71-gae887a0ca3-dirty" | parse version grok "%{GREEDYDATA:version}"'
  check tenzir 'show version | put version="v4.5.0-71-gae887a0ca3-dirty" | parse version grok "v%{INT:major:int}\.%{INT:minor:int}\.%{INT:patch:int}(-%{INT:tweak:int}-%{DATA:ref}(-%{DATA:extra})?)?"'
  check tenzir 'show version | put version="v4.5.0-71-gae887a0ca3-dirty" | parse version grok --indexed-captures "v%{INT:major:int}\.%{INT:minor:int}\.%{INT:patch:int}(-%{INT:tweak}-%{DATA:ref}(-%{DATA:extra})?)?"'
  check tenzir 'show version | put version="v4.5.0-71-gae887a0ca3-dirty" | parse version grok "v(?<major>[0-9]+)\.(?<rest>.*)"'
  check tenzir 'show version | put version="v4.5.0-71-gae887a0ca3-dirty" | parse version grok --pattern-definitions "VERSION %{INT:major:int}\.%{INT:minor:int}\.%{INT:patch:int}" "v%{VERSION}(-%{INT:tweak:int}-%{DATA:ref}(-%{DATA:extra})?)?"'
  check tenzir 'show version | put url="https://example.com/test.txt?foo=bar" | parse url grok --include-unnamed "%{URI}"'
  check tenzir "from ${INPUTSDIR}/syslog/syslog-rfc3164.log read lines | parse line grok \"(<%{NONNEGINT:priority}>\s*)?%{SYSLOGTIMESTAMP:timestamp} (%{HOSTNAME:hostname}/%{IPV4:hostip}|%{WORD:host}) %{SYSLOGPROG}:%{GREEDYDATA:message}\""
  check tenzir "from ${INPUTSDIR}/syslog/syslog-rfc3164.log read lines | parse line grok --include-unnamed \"(<%{NONNEGINT:priority}>\s*)?%{SYSLOGTIMESTAMP:timestamp} (%{HOSTNAME:hostname}/%{IPV4:hostip}|%{WORD:host}) %{SYSLOGPROG}:%{GREEDYDATA:message}\""
  check tenzir 'show version | put version="v4.5.0-71-gae887a0ca3-dirty" | parse version grok "%{TIMESTAMP_ISO8601}"'
  check tenzir 'show version | put line="55.3.244.1 GET /index.html 15824 0.043" | parse line grok "%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}"'
}

# bats test_tags=pipelines
@test "Print JSON in CEF" {
  check tenzir "read cef | slice 1:3 | print extension json | select extension" <${INPUTSDIR}/cef/forcepoint.log
  check ! tenzir "read cef | print extension.dvc json " <${INPUTSDIR}/cef/forcepoint.log
}

# bats test_tags=pipelines
@test "Print CSV in CEF" {
  check tenzir "read cef | slice 1:2 | print extension csv" <${INPUTSDIR}/cef/forcepoint.log
  check tenzir "read cef | slice 1:2 | print extension csv --no-header | select extension" <${INPUTSDIR}/cef/forcepoint.log
}

# bats test_tags=pipelines
@test "Print non UTF8 string" {
  check ! tenzir "read cef | print extension feather" <${INPUTSDIR}/cef/forcepoint.log
  check ! tenzir "read cef | print extension bitz" <${INPUTSDIR}/cef/forcepoint.log
}

@test "Print nested data" {
  check tenzir "read json | print a csv | parse a csv" <${INPUTSDIR}/json/nested-object.json
  check tenzir "read json | print a.b csv" <${INPUTSDIR}/json/nested-object.json
  check tenzir "read json | print a.b zeek-tsv | parse a.b zeek-tsv" <${INPUTSDIR}/json/nested-object.json
  check ! tenzir "read json | print a.b.c json" <${INPUTSDIR}/json/nested-object.json
  check tenzir "read json | print a.b csv | print a yaml" <${INPUTSDIR}/json/nested-object.json
}

@test "Print multiple events" {
  check tenzir "read json | repeat 4 | print a csv | parse a csv" <${INPUTSDIR}/json/nested-object.json
  check ! tenzir "read json | repeat 2 | print a.b.c json" <${INPUTSDIR}/json/nested-object.json
}

# bats test_tags=pipelines, csv
@test "CSV with comments" {
  check tenzir "from ${INPUTSDIR}/csv/ipblocklist.csv read csv --allow-comments"
}

# bats test_tags=pipelines, xsv
@test "XSV Format" {
  check tenzir "from ${INPUTSDIR}/xsv/sample.csv read csv | extend schema=#schema | write csv"
  check tenzir "from ${INPUTSDIR}/xsv/sample.ssv read ssv | extend schema=#schema | write ssv"
  check tenzir "from ${INPUTSDIR}/xsv/sample.tsv read tsv | extend schema=#schema | write tsv"
  check tenzir "from ${INPUTSDIR}/xsv/nulls-and-escaping.csv read csv"
  # Test that multiple batches only print the header once.
  check tenzir "read json --ndjson --precise | select foo | write csv" <<EOF
  {"foo": 1}
  {"foo": 2, "bar": 3}
EOF
}

@test "read xsv auto expand" {
  echo "1,2,3" | check tenzir 'read csv --header "foo,bar,baz"'
  echo "1,2" | check tenzir 'read csv --header "foo,bar,baz"'
  echo "1,2,3,4,5" | check tenzir 'read csv --header "foo,bar,baz"'
  echo "1,2,3,4,5" | check tenzir 'read csv --header "foo,bar,baz" --auto-expand'
  echo "1,2,3,4,5" | check tenzir 'read csv --header "foo,unnamed1,baz" --auto-expand'
}

# bats test_tags=pipelines, xsv
@test "Slice" {
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice 1:"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice -1:"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice :1"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice :-1"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice 1:1"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice 1:2"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice 1:-1"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice -1:1"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice -1:-1"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice -2:-1"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice 1::-2"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice :-1:-5"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice -10:-5:2"
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | head 100 | enumerate | slice ::-1"
}

# bats test_tags=pipelines
@test "Parse KV" {
  check tenzir "from ${INPUTSDIR}/txt/key_value_pairs.txt read lines | parse line kv \"(\s+)[A-Z][A-Z_]+\" \":\s*\""
  check ! tenzir 'parse line kv "(foo)(bar)" ""'
  check ! tenzir 'parse line kv "foo(?=bar)" ""'
}

@test "Parse JSON with numeric timestamp" {
  local schemas="$BATS_RUN_TMPDIR/tmp/$BATS_TEST_NAME"
  mkdir -p $schemas
  local schema="$schemas/foo_bar.schema"
  cat >$schema <<EOF
type foo_bar = record {
  foo: time #unit=ms,
  bar: time #unit=ns,
}
EOF
  check tenzir --schema-dirs=$schemas "from stdin read json --schema=foo_bar" <<EOF
{
  "foo": 1707736115592,
  "bar": 1707736115592000000
}
EOF
}

@test "bitz format" {
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | write bitz | read bitz"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | set #schema=\"foo\" | write bitz | read bitz | set schema=#schema"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | batch 1 | write bitz | read bitz"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | head 1 | repeat 100 | batch 100 | write bitz | read bitz"
}

@test "set operator" {
  check tenzir 'version | set foo="patch", :uint64=-1, :ip=1.1.1.1, qux=foo, version=123, #schema="foo.bar", schema=#schema, build=null | set schema2=#schema, baz=patch'
}

# bats test_tags=pipelines, deduplicate
@test "Deduplicate operator" {
  check tenzir "from ${INPUTSDIR}/json/all-types.json | deduplicate --limit 1"
  check tenzir "from ${INPUTSDIR}/json/all-types.json | deduplicate e --limit 1"
  check tenzir "from ${INPUTSDIR}/json/all-types.json | deduplicate b --limit 1"
  check tenzir "from ${INPUTSDIR}/json/all-types.json | batch 1 | deduplicate b --limit 1"
  check tenzir "from ${INPUTSDIR}/json/all-types.json | deduplicate b --limit 1 --distance 1"
  check tenzir "from ${INPUTSDIR}/json/all-types.json | deduplicate b,e --limit 1"

  check tenzir "from stdin read json | deduplicate :ip --limit 2" <<EOF
{"value": "192.168.1.1", "tag": 1}
{"value": "192.168.1.2", "tag": 2}
{"value": "192.168.1.3", "tag": 3}
{"value": "192.168.1.2", "tag": 4}
{"value": "192.168.1.1", "tag": 5}
{"value": "192.168.1.2", "tag": 6}
{"value": "192.168.1.3", "tag": 7}
{"value": "192.168.1.2", "tag": 8}
{"value": "192.168.1.1", "tag": 9}
EOF

  check tenzir "from stdin read json | deduplicate :ip --distance 3 --limit 1" <<EOF
{"value": "192.168.1.1", "tag": 1}
{"value": "192.168.1.2", "tag": 2}
{"value": "192.168.1.3", "tag": 3}
{"value": "192.168.1.2", "tag": 4}
{"value": "192.168.1.1", "tag": 5}
{"value": "192.168.1.2", "tag": 6}
{"value": "192.168.1.3", "tag": 7}
{"value": "192.168.1.2", "tag": 8}
{"value": "192.168.1.1", "tag": 9}
EOF

  check tenzir "from stdin read json | deduplicate value --limit 1" <<EOF
{"value": 123, "tag": 1}
{"value": null, "tag": 2}
{"value": 123, "tag": 3}
{"tag": 4}
EOF

  check tenzir "from stdin read json | deduplicate foo.bar --limit 1" <<EOF
{"foo": {"bar": 123}, "tag": 1}
{"foo": {"bar": null}, "tag": 2}
{"foo": 123, "tag": 3}
{"foo": {}, "tag": 4}
{"foo": 123, "tag": 5}
{"tag": 6}
{"foo": null, "tag": 7}
{"foo": {"bar": 123}, "tag": 8}
EOF

  check ! tenzir "from stdin read json | deduplicate :ip --limit 1" <<EOF
{"value": 123, "tag": 1}
{"value": null, "tag": 2}
{"value": 123, "tag": 3}
{"tag": 4}
EOF

  check tenzir "from stdin read json | deduplicate a, b --limit 1" <<EOF
{"a": 1, "b": 2, "tag": 1}
{"b": "reset", "tag": 2}
{"b": 2, "a": 1, "tag": 3}
EOF

  check tenzir "from stdin read json | deduplicate --limit 1" <<EOF
{"a": 1, "b": 2}
{"b": "reset"}
{"b": 2, "a": 1}
EOF

  # Potentially flaky, if `tenzir` takes more than (8s - 100ms) to start up:
  # (
  #   echo "{\"value\": \"A\", \"tag\": 1}"
  #   sleep 8s
  #   echo "{\"value\": \"A\", \"tag\": 2}"
  # ) | check tenzir "from stdin read json | deduplicate value --limit 1 --timeout 100ms"
}

# bats test_tags=pipelines
@test "unroll operator" {
  check tenzir 'unroll b' <<EOF
{"a": 1, "b": [1, 2, 3]}
{"a": 2, "b": [1]}
{"a": 3, "b": []}
{"a": 4, "b": null}
EOF

  check tenzir 'unroll conn | where conn.dest !in 192.168.0.0/16 || active > 100ms' <<EOF
{
  "src": "192.168.0.5",
  "conn": [
    {
      "dest": "192.168.0.34",
      "active": "381ms"
    },
    {
      "dest": "192.168.0.120",
      "active": "42ms"
    },
    {
      "dest": "1.2.3.4",
      "active": "67ms"
    }
  ]
}
EOF

  # Make sure that we can duplicate records that contain enumerations.
  check tenzir 'read suricata | unroll vlan' <<EOF
{"event_type": "dns", "vlan": [0, null], "dns": {"type": "answer"}}
EOF

  # Make sure that we can duplicate enumerations that sit next to the unrolled list.
  check tenzir 'read suricata | unroll dns.answers' <<EOF
{"event_type": "dns", "dns": {"type": "answer", "answers": [{}, null]}}
EOF

}

# bats test_tags=json,python_operator
@test "weird json floats" {
  check tenzir -f /dev/stdin <<EOF
version |
python "
  import sys

  self.inf = float(\"inf\");
  self.nan = float(\"nan\");
  self.zero = -0.0
  self.subnormal = sys.float_info.min / 100
" |
put inf, nan, subnormal, zero
EOF
}

@test "unflatten empty record and empty record null" {
  check tenzir 'read json | unflatten' <<EOF
{"foo": {}}
{"foo": null}
EOF
}

@test "precise json" {
  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": "0.042s"}
{"foo": "0.043s", "bar": null}
EOF
}

@test "precise json raw" {
  check tenzir 'read json --ndjson --precise --raw' <<EOF
{"foo": "0.042s"}
{"foo": "0.043s", "bar": [{}]}
EOF
}

@test "precise json overwrite field" {
  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": "0.042s", "foo": 42}
EOF
}

@test "precise json list type conflict" {
  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": [42, "bar"]}
EOF
}

@test "precise json big integer" {
  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": 424242424242424242424242}
EOF
}

@test "precise json incomplete input" {
  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": 42
EOF
}

@test "precise json broken input" {
  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": 42,,,
EOF
}

@test "precise json bad ndjson" {
  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": 42}{"foo": 43}
EOF
}

@test "legacy operator" {
  check ! tenzir --tql2 --dump-pipeline 'legacy'
  check tenzir --tql2 --dump-pipeline 'legacy ""'
  check tenzir --tql2 --dump-pipeline 'legacy "from \"example.json.gz\" | write json"'
  check ! tenzir --tql2 --dump-pipeline 'legacy "this_operator_does_not_exist"'
}
