---
sidebar_custom_props:
  connector:
    loader: true
---

# ftp

Loads bytes via FTP.

## Synopsis

```
ftp <url>
```

## Description

The `ftp` loader downloads a file via the [File Transfer Protocol
(FTP)](https://en.wikipedia.org/wiki/File_Transfer_Protocol).

### `<url>`

The FTP URL.

To provide a username and password, use the typical `user:pass@example.org`
syntax.

The scheme is `ftp://` and can be omitted.

## Examples

Download and process a [CSV](../formats/csv.md) file:

```
from ftp example.org/file.csv read csv
```

Process a Zstd-compressed [Suricata EVE JSON](../formats/suricata.md) file:

```
load ftp example.org/gigantic.eve.json.zst
| decompress zstd
| read suricata
```
