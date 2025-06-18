---
title: "Fix CLI verbosity shorthands"
type: change
authors: mavam
pr: 2178
---

The command line option `--verbosity` has the new name `--console-verbosity`.
This synchronizes the CLI interface with the configuration file that solely
understands the option `vast.console-verbosity`.
