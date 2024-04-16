The `every <duration>` operator modifier now supports all operators, turning
blocking operators like `tail`, `sort` or `summarize` into operators that emit
events every `<duration>`.

The `timeout <duration>` operator modifier works just like `every`, except that
it triggers whenever the operator received no input for the specified duration
as opposed to the operator running for it.
