---
title: Tenzir
section: 1
header: General Commands Manual
author: Tenzir GmbH
---

# NAME

`tenzir` -- manages a Tenzir node

# OVERVIEW

Tenzir is an embeddable security telemetry engine for structured event data.
Tailor-made for security operations, Tenzir is the foundation for many data-driven
detection and response uses cases, such as operationalizing threat intelligence,
threat hunting, event contextualization, and advanced detection engineering.

Tenzir operates in a client-server architecture. You begin with spawning a server
and then interacting with that server through one or more client. The `tenzir`
executable serves both as client and server.

# USAGE

The usage examples in this manpage only scratch the surface. Please consult the
official documentation at https://docs.tenzir.com for a comprehensive user guide.

You get short usage instructions for every `tenzir` command by adding the `help`
sub-command or providing the option `--help` (which has the shorthand `-h`):

```bash
tenzir help
tenzir --help
tenzir -h
```

## Start Tenzir

Use the `start` command to spin up a Tenzir server:

```bash
tenzir start
```

## Ingest data

Use the `import` command to ingest data via standard input, which takes a
*format* as sub-command. For example, to ingest Suricata logs, add `suricata`
after `import`:

```bash
tenzir import suricata < eve.log
```

## Query data

Use the `export` command to run execute query and receive results on standard
output. Like `import`, `export` needs a format as sub-command:

```bash
tenzir export json '6.6.6.6 || (dst_port < 1024 && proto == "UDP")'
```

## Next steps

To learn more about using Tenzir, continue over at https://docs.tenzir.com.

# ISSUES

If you encounter a bug, or have suggestions for improvement, please file an issue
or start a discussion our GitHub repository at https://github.com/tenzir/tenzir.
