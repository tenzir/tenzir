The new `from` operator can be used to easily read in most resources in one go.
For example, you can now write `from "https://example.com/file.json.gz"` and the
operator will automatically deduce the load operator, compression and format.

The new `to` operator can be used to easily send data to most resources in one
go.
For example, you can now write `to "ftps://example.com/file.json.gz"` and the
operator will automatically deduce the save operator, compression and format.
