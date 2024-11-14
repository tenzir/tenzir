# load_ftp

Loads a byte stream via FTP.

```tql
load_ftp url:str
```

## Description

Loads a byte stream via FTP.

### `url: str`

The URL to request from. The `ftp://` scheme can be omitted.

## Examples

```tql
load_ftp "ftp.example.org"
```
