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
