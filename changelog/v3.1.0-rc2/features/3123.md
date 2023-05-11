The `vast exec` command now supports implicit sinks for pipelines that end in
events or bytes: `write json --pretty` and `save file -`, respectively.

The `--pretty` option for the JSON printer enables multi-line output.

The new `version` source operator yields a single event containing VAST's
version and a list of enabled plugins.
