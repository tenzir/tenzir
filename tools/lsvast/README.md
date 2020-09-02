# lsvast

A tool to show a human-readable summary of the data in a vast.db/ folder.

```
  ./lsvast /path/to/vast.db
```

As opposed to `vast status`, this tool will never alter the database directory,
and will attempt to display information even with a partially corrupted database
state.