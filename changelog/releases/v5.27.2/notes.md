This release adds the hmac function for computing Hash-based Message Authentication Codes over strings and blobs. It also fixes an assertion failure in array slicing that was introduced in v5.27.0.

## 🐞 Bug fixes

### Fixed an assertion failure in slicing

We fixed a bug that would cause an assertion failure *"Index error: array slice would exceed array length"*. This was introduced as part of an optimization in Tenzir Node v5.27.0.

*By @IyeOnline in #5842.*
