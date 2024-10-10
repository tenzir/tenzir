The `kv` parser now allows for keys and values to be enclosed in double quotes:
Split matches within quotes will not be considered. Quotes will be trimmed of
keys and values. For example `"key"="nested = value, fun"` will now successfully
parse as `{ "key" : "nested = value, fun" }`.
