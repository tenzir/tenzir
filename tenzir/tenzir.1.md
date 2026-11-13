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
official documentation at https://tenzir.com/docs for a comprehensive user guide.

## Run a pipeline

Use `tenzir run` to run a TQL pipeline locally, passing the pipeline as the
argument:

```bash
tenzir run 'from {message: "hello"} | write_json'
```

A pipeline can read events from standard input and write results to standard
output:

```bash
tenzir run 'read_suricata | where event_type == "alert" | write_json' < eve.json
```

Pass `-f` to read the pipeline from a file instead:

```bash
tenzir run -f pipeline.tql
```

## Get help

Run `tenzir --help` to list all commands and their options:

```bash
tenzir --help
```

Run `tenzir run --help` for the options of a local pipeline run. In the
previous command interface, `tenzir --help` lists only the engine's options, and
`tenzir platform --help` lists the platform commands.

## Next steps

To learn more about using Tenzir, continue over at https://tenzir.com/docs.

# COMMANDS

`tenzir` has two command interfaces. `TENZIR_UNIFIED` selects one: `0`,
`false`, or `no` (case-insensitive) select the previous interface, and any other
value selects the unified one. If `TENZIR_UNIFIED` is unset or empty, Tenzir
v7 and later use the unified interface, and earlier versions the previous one.

In the unified interface, `tenzir run <pipeline>` runs a pipeline locally in the
engine, `tenzir-engine`. With `-r` or `--remote` before an optional `--`, it
runs the pipeline remotely instead. Remote runs and all other commands go to the
bundled platform CLI, `tenzir-platform`, with their arguments unchanged. Both
executables live in the `libexec` directory of the installation. A build without
the platform CLI supports only `tenzir run`.

In the previous interface, `tenzir <pipeline>` runs a pipeline like
`tenzir run <pipeline>`, and takes the same options. `tenzir platform` and
`tenzir up` invoke the platform CLI, the former without the `platform`
argument. All other calls invoke the engine with their arguments unchanged.

The `tenzir-node`, `tenzir-ctl`, and `tenzir-rebuild` compatibility names
always invoke the engine.

# ENVIRONMENT

`tenzir` sets `TENZIR_LIBEXEC` to the directory of the bundled executables
before it starts the platform CLI.

# ISSUES

If you encounter a bug, or have suggestions for improvement, please file an issue
or start a discussion our GitHub repository at https://github.com/tenzir/tenzir.
