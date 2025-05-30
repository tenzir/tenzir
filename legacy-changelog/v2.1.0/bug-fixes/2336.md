The `csv` import no longer crashes when the CSV file contains columns not
present in the selected schema. Instead, it imports these columns as strings.

`vast export csv` now renders enum columns in their string representation
instead of their internal numerical representation.
