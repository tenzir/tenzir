The `infer` command attempts to derive a schema from user input. Upon success,
it prints a schema template to standard output.

The `infer` command allows for inferring schemas for the Zeek TSV and JSON
formats. Note that the input is required to be JSON, and unlike other VAST
commands, JSONL (newline-delimited JSON) is not supported.

Example usage:

```bash
gunzip -c integration/data/json/conn.log.json.gz |
  head -1 |
  vast infer
```

Note that the output of the `vast infer` command still needs to be manually
edited in case there was an ambiguity, as the type system of the data source
format may be less strict than the data model used by VAST. E.g., there is no
way to represent an IP address in JSON other than using a string type.

The `vast infer` command is a good starting point for writing custom schemas,
but is not designed to be a replacement for it.

For more informatio on VAST's data model, head over to our [data model
documentation page](https://docs.tenzir.com/vast/data-model/overview).
