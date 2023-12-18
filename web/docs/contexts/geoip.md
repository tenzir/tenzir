# GeoIP

A [MaxMind](https://www.maxmind.com/en/home)-based IP address lookup that
enriches events with geographical data related to a specified IP address.

## Synopsis

```
context create <name> geoip
context create <name> geoip --db-path <mmdb file path>
context update <name>
context update <name> --db-path <mmdb file path>
context delete <name>
enrich <name> --field <field>
lookup <name> --field <field>
```

## Description

The following options are currently supported for the `geoip` context:

### `--db-path <mmdb file path>`

The path to the MaxMind DB file.

Updating the `db-path` will load the file content.

Writing `context update <name>` without the path will force the context to
reload the DB file content.

### `--field <field>`

The name of the field to use as IP address lookup. Only IP and string values
will work with this context.
