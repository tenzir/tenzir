---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# https

Loads bytes via HTTPS.

## Synopsis

```
https [<method>] <url> [<item>..]
```

## Description

The `https` loader is an alias for the [`http`](http.md) connector with a
default URL scheme of `https`.
