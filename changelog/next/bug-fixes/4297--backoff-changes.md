We fixed a regression that caused excess CPU usage for some operators when idle.
This was most visible with the `subscribe`, `export`, `metrics`, `diagnostics`,
`lookup` and `enrich` operators.
