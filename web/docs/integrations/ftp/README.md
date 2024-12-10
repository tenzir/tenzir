# FTP

Tenzir supports the [File Transfer Protocol
(FTP)](https://en.wikipedia.org/wiki/File_Transfer_Protocol), both downloading
and uploading files.

![FTP](ftp.svg)

FTP consists of two separate TCP connections, one control and one data
connection. This can be tricky for some firewalls and may require special
attention.

:::tip URL Support
The URL schemes `ftp://` and `ftps://` dispatche to
[`load_ftp`](../../tql2/operators/load_ftp.md) and
[`save_ftp`](../../tql2/operators/save_ftp.md) for seamless URL-style use via
[`from`](../../tql2/operators/from.md) and [`to`](../../tql2/operators/to.md).
:::

## Examples

### Download a file from an FTP server

```tql
from "ftp://user:pass@ftp.example.org/path/to/file.json"
```

### Upload events to an FTP server

```tql
from {
  x: 42,
  y: "foo",
}
to "ftp://user:pass@ftp.example.org/a/b/c/events.json.gz"
```
