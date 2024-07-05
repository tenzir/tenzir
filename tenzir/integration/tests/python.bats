# We set a generous timeout because the first test that is running
# has to setup the virtualenv for the operator.
: "${BATS_TEST_TIMEOUT:=120}"

# bats file_tags=python_operator

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# bats test_tags=python
@test "simple field access" {
  check tenzir -f /dev/stdin <<END
    version
    | put a=1, b=2
    | python "self.c = self.a + self.b"
END
}

# bats test_tags=python
@test "nested struct support" {
  check tenzir -f /dev/stdin <<END
    version
    | put a.b=1, b=2
    | unflatten
    | python '
        self.c = {}
        self.c.d = {}
        self.c.d.e = self.a.b + self.b'
END
}

# bats test_tags=python
@test "empty output throws" {
  run ! tenzir -f /dev/stdin <<END
    version
    | put a.b=1, b=2
    | python 'self.clear()'
END
  check grep -v '^\s*File ' <<<"$output"
}

# bats test_tags=python
@test "python operator nested passthrough" {
  check tenzir -f /dev/stdin <<END
    version
    | put a.b = 2
    | unflatten
    | python "pass"
END
}

# bats test_tags=python
@test "python operator nested modification" {
  check tenzir -f /dev/stdin <<END
    version
    | put a.b = 2
    | unflatten
    | python "self.a.b = 3"
END
}

# bats test_tags=python
@test "python operator nested deep modification" {
  check tenzir -f /dev/stdin <<END
    version
    | put a.b.c = 2
    | unflatten
    | python "self.original_c = self.a.b.c; self.a.b.c = 3"
END
}

# bats test_tags=python
@test "python operator nested addition" {
  check tenzir -f /dev/stdin <<END
    version
    | put a.b = 2
    | unflatten
    | python "self.a.c = 2 * self.a.b"
END
}

# bats test_tags=python
@test "python operator field deletion" {
  check tenzir -f /dev/stdin <<END
    put a.b = 1, a.c = 2, d = 3
    | unflatten
    | python "del self.a.b = 3; del self.d"
END
}

# bats test_tags=python
@test "python operator requirements statement" {
  skip "broken with Arrow 16 in the Docker image"
  check tenzir -f /dev/stdin <<END
    version
    | put a=1, b=2
    | python --requirements 'datetime' '
        from datetime import datetime, timedelta
        today = datetime.strptime("2024-04-23", "%Y-%m-%d")
        week_later = today + timedelta(weeks=1)
        self.a = today
        self.b = week_later'
END
}

# bats test_tags=python
@test "python operator deletion" {
  check tenzir -f /dev/stdin <<END
    put a.b = 1, a.c = 2, d = 3
    | unflatten
    | python "del self.a.b = 3; del self.d"
END
}

# bats test_tags=python
@test "python operator json passthrough" {
  cat >${BATS_TEST_TMPDIR}/input.json <<END
  {
    "foo": "bar"
  }
END
  check tenzir -f /dev/stdin <<END
    from file ${BATS_TEST_TMPDIR}/input.json
    | python "pass"
END
}

# bats test_tags=python
@test "python operator advanced type passthrough" {
  cat >${BATS_TEST_TMPDIR}/input.json <<END
  {
    "ip": "127.0.0.1",
    "subnet": "10.0.0.1/8",
    "ip6": "::1",
    "subnet6": "::1/64",
    "list": [1,2,3]
  }
END
  check tenzir -f /dev/stdin <<END
    from file ${BATS_TEST_TMPDIR}/input.json
    | python "pass"
END
}

# bats test_tags=python
@test "python operator advanced type manipulation" {
  cat >${BATS_TEST_TMPDIR}/input.json <<END
  {
    "ip": "127.0.255.255",
    "subnet": "127.0.0.1/8"
  }
END
  check tenzir -f /dev/stdin <<END
    from file ${BATS_TEST_TMPDIR}/input.json
    | python "self.inside = self.ip in self.subnet"
END
}

# bats test_tags=python
@test "python operator suricata passthrough" {
  check tenzir -f /dev/stdin <<END
    from file $INPUTSDIR/suricata/eve.json read suricata
    | python "pass"
END
}

# bats test_tags=python
@test "python operator suricata.dns passthrough" {
  check tenzir -f /dev/stdin <<END
    from file $INPUTSDIR/suricata/rrdata-eve.json read suricata
    | python "pass"
END
}

# bats test_tags=python
@test "python operator suricata.dns list manipulation" {
  check tenzir -f /dev/stdin <<END
    from file $INPUTSDIR/suricata/rrdata-eve.json read suricata
    | where dns.answers != null
    | python "self.first_result = self.dns.answers[0]; self.num_results = len(self.dns.answers)"
END
}

# bats test_tags=python
# @test "python operator suricata.dns nested record in list assignment (xfail)" {
#   check tenzir -f /dev/stdin << END
#     from file $INPUTSDIR/suricata/rrdata-eve.json read suricata
#     | where dns.answers != null
#     | python "self.dns.answers[0].rrname = \"boogle.dom\""
# END
# }

# bats test_tags=python
@test "python operator suricata.dns list assignment" {
  check tenzir -f /dev/stdin <<END
    from file $INPUTSDIR/suricata/rrdata-eve.json read suricata
    | python "if self.dns.grouped.MX is not None: self.dns.grouped.MX[0] = \"boogle.dom\""
END
}

# bats test_tags=python
@test "python operator suricata.dns assignment to field in null record" {
  check tenzir -f /dev/stdin <<END
    from file $INPUTSDIR/suricata/rrdata-eve.json read suricata
    | python "
      if self.dns.grouped.TXT is None:
        self.dns.grouped.TXT = \"text record\"
      "
END
}

# bats test_tags=python
@test "python operator fill partial output with nulls" {
  check tenzir -f /dev/stdin <<END
    from file $INPUTSDIR/suricata/rrdata-eve.json read suricata
    | python "
      if self.dns.answers is not None:
        self.had_answers = True
      else:
        self.did_not_have_answers = True"
END
}

# bats test_tags=python
@test "python operator timestamps" {
  check tenzir -f /dev/stdin <<END
    from file $INPUTSDIR/suricata/eve.json read suricata
    | where #schema == "suricata.flow"
    | python "self.flow.duration = self.flow.end - self.flow.start"
END
}

# bats test_tags=python
@test "python operator file option" {
  echo "self.original_c = self.a.b.c; self.a.b.c = 3" >${BATS_TEST_TMPDIR}/code.py
  check tenzir -f /dev/stdin <<END
    version
    | put a.b.c = 2
    | unflatten
    | python --file ${BATS_TEST_TMPDIR}/code.py
END
}

# bats test_tags=python
@test "python operator empty input" {
  check tenzir 'version | put x=42 | python ""'
}

# bats test_tags=python
@test "python operator invalid syntax" {
  check ! tenzir 'shell "sleep 3; exit 1" | read json | python "x="'
  check ! tenzir 'version | put x=42 | python "@@"'
}

# bats test_tags=python
@test "python operator invalid syntax in file" {
  echo "self.original_c = !!" >${BATS_TEST_TMPDIR}/code.py
  check ! tenzir -f /dev/stdin <<END
  shell "sleep 3; exit 1"
    | read json
    | put a.b.c = 2
    | unflatten
    | python --file ${BATS_TEST_TMPDIR}/code.py
END
}
