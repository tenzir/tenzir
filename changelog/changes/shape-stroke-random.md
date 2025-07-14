---
title: "Fixed `encrypt_cryptopan` function"
type: bugfix
authors: dominiklohmann
pr: 5345
---

We fixed a bug that sometimes caused the `encrypt_cryptopan` function to fail
with the error "got `ip`, expected `ip`", which was caused by an incorrect type
check. The function now works as expected again.
