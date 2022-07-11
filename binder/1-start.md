---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.14.0
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# Getting started

Before going any further, we need to start an instance of the vast server. 

To do so, we are going to download the static binary and start the VAST server process to this kernel.

```python
# Download the static binary of VAST

vast_version="2.1.0"
release_url=f"https://github.com/tenzir/vast/releases/download/v{vast_version}/vast-linux-static.tar.gz"

!curl -sL {release_url} | tar -zx bin/vast
!curl -sL {release_url} | tar -zx share
```

```python
# Start VAST
!bin/vast start
```
