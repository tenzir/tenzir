# GeoIP

A context for enriching IP addresses with geographical data.

## Synopsis

```
context create  <name> geoip [--db-path <mmdb>]
context delete  <name>
context reset   <name> [--db-path <mmdb>]
context save    <name>
context load    <name>
context inspect <name>
enrich <name>
lookup <name>
```

## Description

The `geoip` context uses a [MaxMind](https://www.maxmind.com/) database
to perform IP address lookups.

Run `context reset <name> --db-path <mmdb>` to initialize the database at path
`<mmdb>`. Omitting `--db-path` causes a reload of a previously initialized
database file.

### `--db-path <mmdb>`

The path to the MaxMind database file.

You can provide any database in [MMDB
format](https://maxmind.github.io/MaxMind-DB/).
