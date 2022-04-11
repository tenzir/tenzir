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
`--schema-file` / `-s` as file or to `--schema` / `-S` as string. However, after
`infer` dumps the schema, the generic type name should still be adjusted and
this would be the time to make use of more precise types, such as `timestamp`
instead of `time`, or annotate them with additional attributes, such as `#skip`.

If no type prefix is specified with `--type` / `-t`, or multiple types match
based on the prefix, VAST uses an exact match based on the field names to
automatically deduce the event type for every line in the input.

As an alternative to matching based on the field names, the option `--selector`
allows for specifying a colon-separated field name to type prefix mapping that
allows for selecting the event type based on the value of a field that has to
be present in the data. E.g., `vast import json --selector=event_type:suricata`
is equivalent to the older `vast import suricata`, and reads the value from the
field `event_type`, prefixes it with `suricata.` and uses that as the layout of
the JSONL row.
