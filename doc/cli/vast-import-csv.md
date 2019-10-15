The [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) import format
consumes comma-separated values in tabular form. The first line in a CSV file
must contain a header that describes the field names. The remaining lines
contain concrete values. Except for the header, one line corresponds to one
event.

Because CSV has no notion of typing, it is necessary to select a layout via
`--type`/`-t` whose field names correspond to the CSV header field names.
