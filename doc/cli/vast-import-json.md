The `json` import format consumes [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects
according to a specified schema. That is, one line corresponds to one event.
The object field names correspond to record field names.

JSON's can express only a subset VAST's data model. For example, VAST has
first-class support IP addresses but JSON can only represent them as strings.
To get the most out of your data, it is therefore important to define a schema
to get a differentiated view of the data.

The `infer` command also supports schema inference for JSON data. For example,
`head data.json | vast infer` will print a raw schema that can be supplied to
`--schema-file`/`-s` as file or to `--schema`/`-S` as string. However, after
`infer` dumps the schema, the generic type name should still be adjusted and
this would be the time to annotate fields with additional attributes, such as
`#timestamp` or `#skip`.
