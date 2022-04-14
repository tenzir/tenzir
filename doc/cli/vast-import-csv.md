The `import csv` command imports [comma-separated
values](https://en.wikipedia.org/wiki/Comma-separated_values) in tabular form.
The first line in a CSV file must contain a header that describes the field
names. The remaining lines contain concrete values. Except for the header, one
line corresponds to one event.

Because CSV has no notion of typing, it is necessary to select a layout via
`--type` whose field names correspond to the CSV header field names. Such a
layout must either be defined in a module file known to VAST, or be defined in a
module passed using `--module-file`.

E.g., to import Threat Intelligence data into VAST, the known type
`intel.indicator` can be used:

```bash
vast import --type=intel.indicator --read=path/to/indicators.csv csv
```
