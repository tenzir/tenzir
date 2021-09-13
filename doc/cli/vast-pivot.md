The `pivot` command retrieves data of a related type. It inspects each
event in a query result to find an event of the requested type. If the related
type exists in the schema, VAST will dynamically create a new query to fetch the
contextual data according to the type relationship.

```bash
vast pivot [options] <type> [<expr>]
```

VAST uses the field `community_id` to pivot between logs and packets. Pivoting
is currently implemented for Suricata, Zeek (with [community ID
computation](https://github.com/corelight/bro-community-id) enabled), and PCAP.
For Zeek specifically, the `uid` field is supported as well.

For example, to get all events of type `pcap.packet` that can be pivoted to over
common fields from other events that match the query `dest_ip == 72.247.178.18`,
use this command:

```bash
vast pivot pcap.packet 'dest_ip == 72.247.178.18'
```

The `pivot` command is similar to the `explore` command in that they allow for
querying additional context.

Unlike the `export` command, the output format can be selected using
`--format=<format>`. The default export format is `json`.

For more information on schema pivoting, head over to
[docs.tenzir.com](https://docs.tenzir.com/vast/features/schema-pivoting).
