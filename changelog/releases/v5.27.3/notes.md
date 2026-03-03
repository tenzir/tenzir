This release fixes a crash that could occur when reading JSON data. It also improves CEF parsing to handle non-conforming unescaped equals characters.

## 🐞 Bug fixes

### Fix CEF parsing for unescaped equals

The CEF parser now handles unescaped `=` characters (which are not conforming to the specification) by using a heuristic.

*By @jachris in #5841.*

### JSON reading crash fix

We fixed a bug that could cause a crash when reading JSON data.

*By @IyeOnline in #5855.*
