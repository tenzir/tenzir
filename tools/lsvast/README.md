# lsvast

A tool to show a human-readable summary of the data in a vast.db/ folder.

```bash
$ ./bin/lsvast vast.db/index/fdae6ab3-c44c-42ab-baa1-8e02e02fdc8c
  uuid: fdae6ab3-c44c-42ab-baa1-8e02e02fdc8c
  offset: 16167
  events: 100
  zeek.conn: 100
```

As opposed to `vast status`, this tool will never alter the database directory,
and will attempt to display information even with a partially corrupted database
state.