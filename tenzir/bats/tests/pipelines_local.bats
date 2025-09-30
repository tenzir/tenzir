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




# bats test_tags=pipelines
@test "Read from JSON File" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/json/record-in-list.json read json | write json"
}


# bats test_tags=pipelines
@test "Type mismatch in a column" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/json/type-mismatch.json read json | write json"
}

# bats test_tags=pipelines
@test "Use schema time unit when converting from a double to a duration" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/json/double-to-duration-cast.json read json --selector=schema:argus | select SIntPkt | write json"
}

# bats test_tags=pipelines,json
@test "Read JSON with nested selector field" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/suricata/eve.json read json --selector=flow.start | put x=#schema"
}

# bats test_tags=pipelines,json
@test "Read JSON with integer selector" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/suricata/eve.json read json --selector=pcap_cnt | put x=#schema"
}

# bats test_tags=pipelines
@test "Read from suricata file" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/suricata/eve.json read suricata | write json"
  check tenzir "from file ${INPUTSDIR}/suricata/eve.json read json --schema=suricata.alert --no-infer | write json"
}

# bats test_tags=pipelines
@test "Skip columns that are not in the schema for suricata input with no-infer option" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/suricata/dns-with-no-schema-column.json read suricata --no-infer | select custom_field | write json"
}

# bats test_tags=pipelines
@test "Read from zeek json file" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/zeek/zeek.json read zeek-json | write json"
}

# bats test_tags=json
@test "Read JSON from tshark output" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/pcap/tshark.json"
}

# bats test_tags=json
@test "Read JSON with new field in record list" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/json/record-list-new-field.json read json --merge"
  check tenzir "from file ${INPUTSDIR}/json/record-list-new-field.json"
}

# bats test_tags=json
@test "Read JSON with differents fields in one record list" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/json/record-list-different-fields.json"
}

# bats test_tags=json
@test "Read JSON with list config in overwritten field" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/json/record-list-conflict-field-overwrite.json"
}

# bats test_tags=json
@test "Read JSON record list with nulls and conflict" {
  export TENZIR_LEGACY=true

  check tenzir "from file ${INPUTSDIR}/json/record-list-with-null-conflict.json"
}

# bats test_tags=pipelines, cef
@test "Schema ID Extractor" {
  export TENZIR_LEGACY=true

  check -c "cat ${INPUTSDIR}/cef/forcepoint.log | tenzir 'read cef | put fingerprint = #schema_id | write json'"

  check -c "cat ${INPUTSDIR}/cef/forcepoint.log | tenzir 'read cef | where #schema_id == \"59e472ba9bb9e014\" | write json'"

  check -c "cat ${INPUTSDIR}/cef/forcepoint.log | tenzir 'read cef | where #schema_id != \"59e472ba9bb9e014\" | write json'"
}

# bats test_tags=pipelines
@test "Batch Events" {
  export TENZIR_LEGACY=true

  check tenzir 'version | repeat 10 | batch 5 | measure | select events'
  check tenzir 'version | repeat 10 | batch 1 | measure | select events'
  check tenzir 'version | repeat 10 | batch 3 | measure | select events'
  check tenzir 'version | repeat 10 | batch 15 | measure | select events'
}

# bats test_tags=pipelines
@test "Heterogeneous Lists" {
  export TENZIR_LEGACY=true

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

# bats test_tags=pipelines
@test "Slice Regression Test" {
  export TENZIR_LEGACY=true

  # This tests for a bug fixed by tenzir/tenzir#3171 that caused sliced nested
  # arrays to be accessed incorrectly, resulting in a crash. The head 8 and tail
  # 3 operators are intentionally chosen to slice in the middle of a batch.
  check tenzir "from ${INPUTSDIR}/cef/forcepoint.log read cef | select extension.dvc | head 8 | extend foo=extension.dvc | write json"
  check tenzir "from ${INPUTSDIR}/cef/forcepoint.log read cef | select extension.dvc | tail 3 | extend foo=extension.dvc | write json"
  # This tests for a regression where slice 1:-1 crashes for exactly one event.
}

# bats test_tags=pipelines, zeek
@test "Flatten Operator" {
  export TENZIR_LEGACY=true

  check tenzir "from ${INPUTSDIR}/json/nested-object.json read json | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/nested-structure.json read json | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-in-list.json read json | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/suricata/eve.json read suricata | flatten | to stdout"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | flatten | to stdout"

  # TODO: Reenable tests with only record flattening.
  # check tenzir "from ${INPUTSDIR}/json/nested-structure.json read json | flatten -l | to stdout"
  # check tenzir "from ${INPUTSDIR}/json/record-in-list.json read json | flatten -l | to stdout"
}

# bats test_tags=pipelines
@test "Unflatten Operator" {
  export TENZIR_LEGACY=true

  check tenzir "from ${INPUTSDIR}/json/record-in-list-in-record.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/records-in-nested-lists.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/records-in-nested-record-lists.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-in-list.json read json | flatten | unflatten | write json"
  check tenzir "from ${INPUTSDIR}/json/nested-object.json read json | flatten | unflatten | write json"
  check tenzir "from ${INPUTSDIR}/json/nested-structure.json read json | flatten | unflatten | write json"
  check tenzir "from ${INPUTSDIR}/json/record-in-list2.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-with-multiple-unflattened-values.json read json | unflatten | to stdout"
  check tenzir "from ${INPUTSDIR}/json/record-with-multi-nested-field-names.json read json | unflatten | to stdout"
  check tenzir "unflatten" <<EOF
{}
EOF
  # {x.y: int64, x: {}}
  check tenzir "read json --merge | unflatten" <<EOF
{"x.y": 1, "x": {}}
{"x.y": null, "x": {}}
{"x.y": 1, "x": null}
{"x.y": null, "x": null}
EOF
  # {x.y: int64, x: {z: int64}}
  check tenzir "read json --merge | unflatten" <<EOF
{"x.y": 1, "x": {"z": 2}}
{"x.y": null, "x": {"z": 2}}
{"x.y": 1, "x": {"z": null}}
{"x.y": 1, "x": null}
{"x.y": null, "x": null}
EOF
  # {x.y: {z: int64}, x: {y.z: int64}}
  check tenzir "read json --merge | unflatten" <<EOF
{"x.y": {"z": 1}, "x": {"y.z": 2}}
{"x.y": null, "x": {"y.z": 2}}
{"x.y": {"z": 1}, "x": null}
{"x.y": null, "x": null}
EOF
  # {x.y: {z: {a.b: int64}}, x: {y.z: {a.c: int64}}}
  check tenzir "read json --merge | unflatten" <<EOF
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
  export TENZIR_LEGACY=true

  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --compact-output"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --omit-nulls"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --omit-empty-objects"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --omit-empty-lists"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | write json --omit-empty"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | flatten | write json"
  check tenzir "from ${INPUTSDIR}/suricata/rrdata-eve.json read suricata | head 1 | flatten | write json --omit-empty"
}

# bats test_#tags=pipelines
@test "S3 Connector" {
  export TENZIR_LEGACY=true

  # TODO: Set up Tenzir S3 stuff for Tenzir-internal read/write tests?
  # TODO: Re-enable this test once Arrow updated their bundled AWS SDK from version 1.10.55,
  # see: https://github.com/apache/arrow/issues/37721
  skip "Disabled due to arrow upstream issue"

  check tenzir 'from s3 s3://sentinel-cogs/sentinel-s2-l2a-cogs/1/C/CV/2023/1/S2B_1CCV_20230101_0_L2A/tileinfo_metadata.json | write json'
}

# bats test_tags=pipelines
@test "Get and Set Attributes" {
  export TENZIR_LEGACY=true

  check tenzir 'version | set-attributes --foo bar --abc=def | get-attributes'
  check tenzir 'version | set-attributes --first 123 | set-attributes --second 456 | get-attributes'
}

# bats test_tags=pipelines,chart
@test "Chart Arguments" {
  export TENZIR_LEGACY=true

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


# bats test_tags=pipelines, syslog
@test "Syslog format" {
  export TENZIR_LEGACY=true

  check tenzir "from ${INPUTSDIR}/syslog/syslog.log read syslog"
  check tenzir "from ${INPUTSDIR}/syslog/syslog-rfc3164.log read syslog"
  check tenzir "from ${INPUTSDIR}/syslog/multiline.log read syslog"
}

# bats test_tags=pipelines
@test "Parse CEF in JSON in JSON from Lines" {
  export TENZIR_LEGACY=true

  check tenzir "from ${INPUTSDIR}/json/cef-in-json-in-json.json read lines"
  check tenzir "from ${INPUTSDIR}/json/cef-in-json-in-json.json read lines | parse line json"
  check tenzir "from ${INPUTSDIR}/json/cef-in-json-in-json.json read lines | parse line json | parse line.foo json"
  check tenzir "from ${INPUTSDIR}/json/cef-in-json-in-json.json read lines | parse line json | parse line.foo json | parse line.foo.baz cef"
}

# bats test_tags=pipelines
@test "Parse CEF over syslog" {
  export TENZIR_LEGACY=true

  check tenzir "from ${INPUTSDIR}/syslog/cef-over-syslog.log read syslog"
  check tenzir "from ${INPUTSDIR}/syslog/cef-over-syslog.log read syslog | parse content cef"
}

# bats test_tags=pipelines
@test "Parse with Grok" {
  export TENZIR_LEGACY=true

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

# bates test_tags=pipelines
@test "Read Grok" {
  export TENZIR_LEGACY=true

  echo "v4.5.0-71-gae887a0ca3-dirty" | check tenzir 'read grok "%{GREEDYDATA:version}"'
  echo "v4.5.0-71-gae887a0ca3-dirty" | check tenzir 'read grok "v%{INT:major:int}\.%{INT:minor:int}\.%{INT:patch:int}(-%{INT:tweak:int}-%{DATA:ref}(-%{DATA:extra})?)?"'
  echo "v4.5.0-71-gae887a0ca3-dirty" | check tenzir 'read grok --indexed-captures "v%{INT:major:int}\.%{INT:minor:int}\.%{INT:patch:int}(-%{INT:tweak}-%{DATA:ref}(-%{DATA:extra})?)?"'
  echo "https://example.com/test.txt?foo=bar" | check tenzir 'read grok --include-unnamed "%{URI}"'
  echo "55.3.244.1 GET /index.html 15824 0.043" | check tenzir 'read grok "%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}"'
  echo "v4.5.0-71-gae887a0ca3-dirty" | check tenzir 'read grok "%{TIMESTAMP_ISO8601}"'
}

# bats test_tags=pipelines
@test "Print JSON in CEF" {
  export TENZIR_LEGACY=true

  check tenzir "read cef | slice 1:3 | print extension json | select extension" <${INPUTSDIR}/cef/forcepoint.log
  check ! tenzir "read cef | print extension.dvc json " <${INPUTSDIR}/cef/forcepoint.log
}

# bats test_tags=pipelines
@test "Print CSV in CEF" {
  export TENZIR_LEGACY=true

  check tenzir "read cef | slice 1:2 | print extension csv" <${INPUTSDIR}/cef/forcepoint.log
  check tenzir "read cef | slice 1:2 | print extension csv --no-header | select extension" <${INPUTSDIR}/cef/forcepoint.log
}

# bats test_tags=pipelines
@test "Print non UTF8 string" {
  export TENZIR_LEGACY=true

  check ! tenzir "read cef | print extension feather" <${INPUTSDIR}/cef/forcepoint.log
  check ! tenzir "read cef | print extension bitz" <${INPUTSDIR}/cef/forcepoint.log
}

@test "Print nested data" {
  export TENZIR_LEGACY=true

  check tenzir "read json | print a csv | parse a csv" <${INPUTSDIR}/json/nested-object.json
  check tenzir "read json | print a.b csv" <${INPUTSDIR}/json/nested-object.json
  check ! tenzir "read json | print a.b.c json" <${INPUTSDIR}/json/nested-object.json
  check tenzir "read json | print a.b csv | print a yaml" <${INPUTSDIR}/json/nested-object.json
}

@test "Print multiple events" {
  export TENZIR_LEGACY=true

  check tenzir "read json | repeat 4 | print a csv | parse a csv" <${INPUTSDIR}/json/nested-object.json
  check ! tenzir "read json | repeat 2 | print a.b.c json" <${INPUTSDIR}/json/nested-object.json
}

# bats test_tags=pipelines, csv
@test "CSV with comments" {
  export TENZIR_LEGACY=true

  check tenzir "from ${INPUTSDIR}/csv/ipblocklist.csv read csv --allow-comments"
}

# bats test_tags=pipelines, xsv
@test "XSV Format" {
  export TENZIR_LEGACY=true

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
  export TENZIR_LEGACY=true

  echo "1,2,3" | check tenzir 'read csv --header "foo,bar,baz"'
  echo "1,2" | check tenzir 'read csv --header "foo,bar,baz"'
  echo "1,2,3,4,5" | check tenzir 'read csv --header "foo,bar,baz"'
  echo "1,2,3,4,5" | check tenzir 'read csv --header "foo,bar,baz" --auto-expand'
  echo "1,2,3,4,5" | check tenzir 'read csv --header "foo,unnamed1,baz" --auto-expand'
}

@test "Parse JSON with numeric timestamp" {
  export TENZIR_LEGACY=true

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
  export TENZIR_LEGACY=true

  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | write bitz | read bitz"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | set #schema=\"foo\" | write bitz | read bitz | set schema=#schema"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | batch 1 | write bitz | read bitz"
  check tenzir "from ${INPUTSDIR}/json/all-types.json read json | head 1 | repeat 100 | batch 100 | write bitz | read bitz"
}

# bats test_tags=pipelines, deduplicate
@test "Deduplicate operator" {
  check tenzir "deduplicate value, limit=2" <<EOF
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

  check tenzir "deduplicate value, distance=3, limit=1" <<EOF
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

  check tenzir "deduplicate value, limit=1" <<EOF
{"value": 123, "tag": 1}
{"value": null, "tag": 2}
{"value": 123, "tag": 3}
{"tag": 4}
EOF

  check tenzir "deduplicate foo.bar, limit=1" <<EOF
{"foo": {"bar": 123}, "tag": 1}
{"foo": {"bar": null}, "tag": 2}
{"foo": 123, "tag": 3}
{"foo": {}, "tag": 4}
{"foo": 123, "tag": 5}
{"tag": 6}
{"foo": null, "tag": 7}
{"foo": {"bar": 123}, "tag": 8}
EOF

  check tenzir "deduplicate {a: a, b: b}, limit=1" <<EOF
{"a": 1, "b": 2, "tag": 1}
{"b": "reset", "tag": 2}
{"b": 2, "a": 1, "tag": 3}
EOF

  check tenzir "deduplicate limit=1" <<EOF
{"a": 1, "b": 2}
{"b": "reset"}
{"b": 2, "a": 1}
EOF
}

# bats test_tags=pipelines
@test "unroll operator" {
  # Test how we unroll records, including their various options for null fields.
  check tenzir -f /dev/stdin <<EOF
from {
  events: [
    {foo: 1, bar: {baz: 2, qux: 3}},
    {foo: 2, bar: null},
    {foo: 3, bar: {baz: null, qux: 3}},
    {foo: 4, bar: {baz: 4, qux: null}},
    {foo: 5, bar: {baz: 5, qux: 6}}
  ]
}
unroll events
this = events
unroll bar
EOF
  check tenzir -f /dev/stdin <<EOF
from {
  events: [
    {foo: 1, bar: {baz: 2, qux: 3}},
    {foo: 2, bar: null},
    {foo: 3, bar: {baz: null, qux: 3}},
    {foo: 4, bar: {baz: 4, qux: null}},
    {foo: 5, bar: {baz: 5, qux: 6}}
  ]
}
unroll events
this = events
unordered {
  unroll bar
}
EOF
  check tenzir -f /dev/stdin <<EOF
from {
  events: [
    {foo: 1, bar: 2, baz: 3},
    {foo: 4, bar: null, baz: 5},
  ]
}
unroll events
this = events
unroll this
EOF
  check tenzir -f /dev/stdin <<EOF
from {
  events: [
    {foo: 1, bar: 2, baz: 3},
    {foo: 4, bar: null, baz: 5},
  ]
}
unroll events
this = events
unordered {
  unroll this
}
EOF
}

@test "unflatten empty record and empty record null" {
  export TENZIR_LEGACY=true

  check tenzir 'read json | unflatten' <<EOF
{"foo": {}}
{"foo": null}
EOF
}

@test "precise json" {
  export TENZIR_LEGACY=true

  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": "0.042s"}
{"foo": "0.043s", "bar": null}
EOF
}

@test "precise json raw" {
  export TENZIR_LEGACY=true

  check tenzir 'read json --ndjson --precise --raw' <<EOF
{"foo": "0.042s"}
{"foo": "0.043s", "bar": [{}]}
EOF
}

@test "precise json overwrite field" {
  export TENZIR_LEGACY=true

  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": "0.042s", "foo": 42}
EOF
}

@test "precise json list type conflict" {
  export TENZIR_LEGACY=true

  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": [42, "bar"]}
EOF
}

@test "precise json big integer" {
  export TENZIR_LEGACY=true

  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": 424242424242424242424242}
EOF
}

@test "precise json incomplete input" {
  export TENZIR_LEGACY=true

  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": 42
EOF
}

@test "precise json broken input" {
  export TENZIR_LEGACY=true

  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": 42,,,
EOF
}

@test "precise json bad ndjson" {
  export TENZIR_LEGACY=true

  check tenzir 'read json --ndjson --precise' <<EOF
{"foo": 42}{"foo": 43}
EOF
}

@test "legacy operator" {
  check ! tenzir --dump-pipeline 'legacy'
  check tenzir --dump-pipeline 'legacy ""'
  check tenzir --dump-pipeline 'legacy "from \"example.json.gz\" | write json"'
  check ! tenzir --dump-pipeline 'legacy "this_operator_does_not_exist"'
}


@test "zip" {
  check tenzir 'from {foo: [1, 2, 3], bar: [4, 5, 6]} | baz = zip(foo, bar)'
  check tenzir 'from {foo: [1, 2, 3], bar: [4, 5]} | baz = zip(foo, bar)'
  check tenzir 'from {foo: [], bar: [4, 5]} | baz = zip(foo, bar)'
  check tenzir 'from {foo: [], bar: []} | baz = zip(foo, bar)'
  check tenzir 'from {foo: null, bar: [1]} | baz = zip(foo, bar)'
  check tenzir 'from {foo: null, bar: null} | baz = zip(foo, bar)'
}
