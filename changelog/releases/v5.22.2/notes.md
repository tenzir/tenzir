This release fixes a performance regression when parsing lists with mixed-type elements, where batch processing was inadvertently broken. It also resolves an assertion failure that could crash Tenzir when encountering events with duplicate keys.

## 🐞 Bug fixes

### Fixed assertion failure when encountering duplicate keys

We fixed an assertion failure and subsequent crash that could occur when parsing events that contain duplicate keys.

*By @IyeOnline in #5612.*

### Improved Type Conflict Handling

We resolved an issue that would appear when reading in lists (e.g. JSON `[]`) where the elements had different types. Tenzir's type system at this time only supports storing a single type in a list. Our parsers resolve this issue by first attempting conversions (e.g. to a common numeric type) and turning all values into strings as a last resort. Previously this would however also break Tenzir's batch processing leading to significant performance loss. This has now been fixed.

*By @IyeOnline in #5612.*
