Most parsers now properly adhere to schema of the read data. Previously, parsers
would merge heterogeneous input into a single, growing schema, inserting nulls
for fields that did not exist in an event.

Most parsers now support `--schema`, `--selector` and various other flags
to parse their data according to some predefined schema.