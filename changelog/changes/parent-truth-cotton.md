---
title: "Run pipelines with uvx tenzir"
type: feature
authors: tobim
pr: 5482, 5588, 5589
---

The `tenzir` binary is now bundled directly with the `tenzir` Python wheel.
This means you can run Tenzir pipelines on any machine with uv installed,
without any separate installation steps.

Just use `uvx`:

```bash
uvx tenzir 'version'
```

The bundled binary is available for Apple Silicon Macs, aarch64 Linux, and
x86_64 Linux. On other platforms, the wheel only contains the Python bindings
and you need to install the `tenzir` binary separately.
