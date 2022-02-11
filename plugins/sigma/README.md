# Sigma Plugin for VAST

This query language plugin makes it possible to provide a
[Sigma](https://github.com/Neo23x0/sigma) rule as an alternative to a VAST
query expression. VAST parses the YAML and translates the Sigma rule into a
native query plan to run the search.

The easiest way to execute a Sigma rule is providing it on standard input:

```bash
vast export json < sigma-rule.yaml
```

This is the equivalent of typing out the whole as last argument on the command
line, i.e., if there is no query, VAST assumes the user provides it via stdin.
In the above example, VAST renders the result as JSON, but the choice of output
format is independent of the query input format.

For detailed usage instructions, please consult the [VAST
documentation](https://docs.tenzir.com/vast/query-language/sigma).
