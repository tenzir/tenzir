The `get` command retrieves events of specific ids that were assigned to them
when they were imported.

```bash
vast get [options] [ids]
```

Let's look at an example:

```bash
vast get 0 42 1234
```

The above command outputs the requested events in JSON format. Other formatters
can be selected with the `--format` option.
