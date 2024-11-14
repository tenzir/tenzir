# save_ftp

Saves a byte stream via FTP.

```tql
save_ftp url:str
```

## Description

Saves a byte stream via FTP.

### `url: str`

The URL to request from. The `ftp://` scheme can be omitted.

## Examples

```tql
save_ftp "ftp.example.org"
```
