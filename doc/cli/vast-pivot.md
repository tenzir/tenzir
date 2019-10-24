The `pivot` command retrieves data of a related type. It inspects each event in
a query result to find an event of the requested type. If the related type
exists in the schema, VAST will dynamically create a new query to fetch the
contextual data according to the type relationship.

```sh
vast pivot [options] <type> <expr>
```

VAST uses the field `community_id` to pivot between logs and packets. Pivoting
is currently implemented for Suricata, Zeek (with [community ID computation]
(https://github.com/corelight/bro-community-id) enabled), and PCAP.
