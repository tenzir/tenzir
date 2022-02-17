VAST has a new transform API: the `apply()` function is changed to `add()` and
`finish()`. The `add()` function receives arrow batches instead of
`table_slice`s. Multiple calls to the `add()` function adds multiple batches to
the transformation, and the `finish()` function returns the transformed slices;
this enables the transform to change multiple table slices simultaneously.
