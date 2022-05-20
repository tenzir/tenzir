---
title: VAST
section: 1
header: General Commands Manual
author: Tenzir GmbH
---

# NAME

`vast` -- manages a VAST node

# OVERVIEW

VAST is an embeddable security telemetry engine for structured event data.
Tailor-made for security operations, VAST is the foundation for many data-driven
detection and response uses cases, such as operationalizing threat intelligence,
threat hunting, event contextualization, and advanced detection engineering.

VAST operates in a client-server architecture. You begin with spawning a server
and then interacting with that server through one or more client. The `vast`
executable serves both as client and server.

# USAGE

The usage examples in this manpage only scratch the surface. Please consult the
official documentation at https://vast.io/docs for a comprehensive user guide.

You get short usage instructions for every `vast` command by adding the `help`
sub-command or providing the option `--help` (which has the shorthand `-h`):

```bash
vast help
vast --help
vast -h
```

## Start VAST

Use the `start` command to spin up a VAST server:

```bash
vast start
```

## Ingest data

Use the `import` command to ingest data via standard input, which takes a
*format* as sub-command. For example, to ingest Suricata logs, add `suricata`
after `import`:

```bash
vast import suricata < eve.log
```

## Query data

Use the `export` command to run execute query and receive results on standard
output. Like `import`, `export` needs a format as sub-command:

```bash
vast export json '6.6.6.6 || (dst_port < 1024 && proto == "UDP")'
```

## Next steps

To learn more about using VAST, continue over at https://vast.io/docs/use-vast.

# ISSUES

If you encounter a bug, or have suggestions for improvement, please file an issue
or start a discussion our GitHub repository at https://github.com/tenzir/vast.
