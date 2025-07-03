: "${BATS_TEST_TIMEOUT:=120}"

# Tests for the TQL pretty printer formatter.
# The formatter transforms messy TQL code into clean, readable format.

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# -- tests --------------------------------------------------

# bats test_tags=formatter
@test "Format simple pipeline" {
  input='from"data.json"|where name=="John"|select name,age,email|head 10'
  expected='from "data.json"
where name == "John"
select name, age, email
head 10'
  
  run tenzir --dump-formatted <<< "$input"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter
@test "Format nested structures" {
  input='event={timestamp:now(),user:{name:"Alice",age:30},tags:["admin","user"]}if score>100{priority=1}else{priority=3}'
  expected='event = {
  timestamp: now(), user: {
    name: "Alice", age: 30
  }, tags: ["admin", "user"]
}
if score > 100 {
  priority = 1
} else {
  priority = 3
}'

  run tenzir --dump-formatted <<< "$input"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter
@test "Format with comments and lets" {
  input='//Setup data
let threshold=100/*important value*/
from"events.json"|where value>threshold//filter important events
|select name,value'
  expected='// Setup data
let threshold = 100 /* important value */
from "events.json"
where value > threshold // filter important events
select name, value'

  run tenzir --dump-formatted <<< "$input"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter
@test "Format complex expressions" {
  input='where(score>50and active==true)or(priority>=2and type in["critical","high"])'
  expected='where (score > 50 and active == true) or (priority >= 2 and type in ["critical", "high"])'

  run tenzir --dump-formatted <<< "$input"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter
@test "Format function calls and methods" {
  input='select name.upper(),timestamp.format("%Y-%m-%d"),data.field_names()head 5|enumerate start=1'
  expected='select name.upper(), timestamp.format("%Y-%m-%d"), data.field_names()
head 5
enumerate start = 1'

  run tenzir --dump-formatted <<< "$input"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter
@test "Format string literals with prefixes" {
  input='let pattern=r"[a-z]+"let data=b"binary"select pattern'
  expected='let pattern = r"[a-z]+"
let data = b"binary"
select pattern'

  run tenzir --dump-formatted <<< "$input"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter
@test "Format control flow structures" {
  input='if condition{action1|action2}else{alternative}let result=if success then"ok"else"fail"'
  expected='if condition {
  action1
  action2
} else {
  alternative
}
let result = if success then "ok" else "fail"'

  run tenzir --dump-formatted <<< "$input"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter
@test "Format comprehensive pipeline" {
  input='from"logs.json"|parse_json field="message"|where severity in["error","critical"]and@timestamp>now()-1h|let processed_time=now()|select@timestamp,severity,message,processed_time|sort@timestamp desc|head 100'
  expected='from "logs.json"
parse_json field = "message"
where severity in ["error", "critical"] and @timestamp > now() - 1h
let processed_time = now()
select @timestamp, severity, message, processed_time
sort @timestamp desc
head 100'

  run tenzir --dump-formatted <<< "$input"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter,file
@test "Format from file" {
  # Create a temporary messy TQL file
  local input_file="${BATS_TEST_TMPDIR}/messy.tql"
  local expected='from "data.json"
where name == "test"
head 5'
  
  echo 'from"data.json"|where name=="test"|head 5' > "$input_file"
  
  run tenzir --dump-formatted -f "$input_file"
  assert_success
  assert_output "$expected"
}

# bats test_tags=formatter,error
@test "Format handles syntax errors gracefully" {
  # Test with invalid TQL - should fail gracefully
  input='from "data.json" where ||| invalid'
  
  run tenzir --dump-formatted <<< "$input"
  assert_failure
  # Should contain some error indication
  assert_output --partial "error"
}