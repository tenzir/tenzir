---
title: Offline Python operator
type: change
authors:
  - tobim
created: 2026-08-23T08:06:47.278825Z
---

Basic usage of the `python` operator now works without an internet connection.
All packages bundle the operator's complete Python dependency set as wheels, so
setting up the operator's environment no longer downloads anything from PyPI.
The container images additionally pin the operator to their bundled Python
interpreter, which previously had to be fetched from the network on first use.

The operator's environment now always matches the bundled wheels: packages
record the Python version the wheels were built for, and the operator provisions
a matching interpreter on systems whose default Python differs. When no matching
interpreter is available and none can be downloaded, the operator falls back to
the interpreter it finds and resolves its dependencies from PyPI, emitting a
warning. Setting `UV_PYTHON` still overrides the interpreter choice.

Passing extra packages via the operator's `requirements` option still requires
network access to fetch them. Such packages now work inside the container
images, which previously lacked the system libraries that prebuilt Python
wheels expect.

Despite the bundled wheels, the container images shrink by roughly 400 MB: the
Python environment they carried existed only to back the operator's
dependencies and is gone now that the wheels are self-sufficient.

The `tenzir` wheel on PyPI keeps its size: it ships only the portable subset of
the bundle, and the operator resolves the binary dependencies from PyPI - a
pip-installed node runs on a machine with internet access by definition.
