The new `from <connector> [read <format>]`, `read <format> [from <connector>]`,
`write <format> [to <connector>]`, and `to <connector> [write <format>]`
operators bring together a connector and a format to prduce and consume events,
respectively. Their lower-level building blocks `load <connector>`, `parse
<format>`, `print <format>`, and `save <connector>` enable expert users to
operate on raw byte streams directly.
