# yara

Executes Yara rules on byte streams.

## Synopsis

```
yara [-C|--compiled-rules] [-f|--fast-scan] <rule> [<rule>..]
```

## Description

The `yara` operator applies [Yara](https://virustotal.github.io/yara/) rules to
an input of bytes, emitting rule context upon a match.

We modeled the operator after the official [`yara` command-line
utility](https://yara.readthedocs.io/en/stable/commandline.html) to enable a
familiar experience for the command users. Similar to the official `yara`
command, the operator compiles the rules by default, unless you provide the
option `-C,--compiled-rules`. To quote from the above link:

> This is a security measure to prevent users from inadvertently using compiled
> rules coming from a third-party. Using compiled rules from untrusted sources
> can lead to the execution of malicious code in your computer.

### `-C|--compiled-rules`

Interpret the rules as compiled.

When providing this flag, you must exactly provide one rule path as positional
argument.

### `-f|--fast-scan`

Enable fast matching mode.

### `<rule>`

The path to the Yara rule(s).

If the path is a directory, the operator attempts to recursively add all
contained files as Yara rules.

## Examples

Scan a file with a set of Yara rules:

```
load file --mmap evil | yara rule.yara
```

:::caution
Note the `--mmap` flag: we need it to deliver a single block of data to the
`yara` operator. Without `--mmap`, the [`file`](../../connectors/file.md) loader
will generate a stream of byte chunks and feed them incrementally to `yara`,
attempting to match `rule.yara` individualy on *every chunk*. This will cause
false negatives when rule matches would span chunk boundaries.
:::

Let's unpack a concrete example:

```yara
rule test {
  meta:
    string = "string meta data"
    integer = 42
    boolean = true

  strings:
    $foo = "foo"
    $bar = "bar"

  condition:
    $foo and $bar
}
```

You can produce test matches by feeding bytes into the `yara` operator:

```bash
echo 'foo barbar baz' |
  tenzir 'load stdin | yara /tmp/test.yara'
```

The resulting `yara.match` events look as follows:

```json
{
  "rule": {
    "identifier": "test",
    "namespace": "default",
    "string": "foo",
    "tags": [],
    "meta": [
      {
        "key": "string",
        "value": "\"string meta data\""
      },
      {
        "key": "integer",
        "value": "42"
      },
      {
        "key": "boolean",
        "value": "true"
      }
    ],
    "matches": [
      {
        "identifier": "$foo",
        "data": "foo",
        "base": 0,
        "offset": 0,
        "match_length": 3,
        "xor_key": 0
      }
    ]
  }
}
{
  "rule": {
    "identifier": "test",
    "namespace": "default",
    "string": "bar",
    "tags": [],
    "meta": [
      {
        "key": "string",
        "value": "\"string meta data\""
      },
      {
        "key": "integer",
        "value": "42"
      },
      {
        "key": "boolean",
        "value": "true"
      }
    ],
    "matches": [
      {
        "identifier": "$bar",
        "data": "bar",
        "base": 0,
        "offset": 4,
        "match_length": 3,
        "xor_key": 0
      },
      {
        "identifier": "$bar",
        "data": "bar",
        "base": 0,
        "offset": 7,
        "match_length": 3,
        "xor_key": 0
      }
    ]
  }
}
```
