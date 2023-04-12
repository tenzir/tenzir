# unique

Removes adjacent duplicates.

## Synopsis

```
unique
```

## Example

Let us assume the following input events (using the JSON format):

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
{"foo": null, "bar": "b"}
{"bar": "b"}
{"foo": null, "bar": "b"}
{"foo": null, "bar": "b"}
```

The `unique` operator removes adjacent duplicates such that the output is:

```json
{"foo": 1, "bar": "a"}
{"foo": 1, "bar": "b"}
{"foo": null, "bar": "b"}
{"bar": "b"}
{"foo": null, "bar": "b"}
```

Note that the output still contains the event `{"foo": null, "bar": "b"}` twice.
This is because `unique` only removes *adjacent* duplicates.

If we want to remove *all* duplicates instead (including non-adjacent ones), we
can use `sort | unique` instead. The `sort` operator ensures that all duplicates
are adjacent and are thus removed by `unique`. Because sorting is a blocking
operation, the output events will only start arriving after the input is done.
