# velociraptor

Submits VQL to a Velociraptor server and returns the response as events.

## Synopsis

```
velociraptor [-n|--request-name <string>] [-o|--org-id <string>]
             [-r|--max-rows <uint64>] [-w|--max-wait <uint64>] <vql>
```

## Description

The `velociraptor` source operator provides a request-response interface to a
[Velociraptor](https://docs.velociraptor.app) server:

![Velociraptor](velociraptor.excalidraw.svg)

The pipeline operator is the client and it establishes a connection to a
Velociraptor server. The client request contains a query written in the
[Velociraptor Query Language (VQL)](https://docs.velociraptor.app/docs/vql/), a
SQL-inspired language with a `SELECT .. FROM .. WHERE` structure.

All Velociraptor client-to-server communication is mutually authenticated and
encrypted via TLS client and server certificates. This means you must provide
client-side key material to connect.

The following steps illustrate how you can create the certificate in
order to use `velociraptor` as API client.

1. Create a server configuration `server.yaml`:
   ```bash
   velociraptor config generate > server.yaml
   ```

1. Create a `tenzir` user with `api` role:
   ```bash
   velociraptor -c server.yaml user add --role=api tenzir
   ```

1. Run the frontend with the server configuration:
   ```bash
   velociraptor -c server.yaml frontend
   ```

1. Create an API client:
   ```bash
   velociraptor -c server.yaml config api_client --name tenzir client.yaml
   ```

Finally, copy the generated `client.yaml` to your Tenzir plugin configuration
directory as `velociraptor.yaml` so that the operator can find it:

```bash
cp client.yaml /etc/tenzir/plugin/velociraptor.yaml
```

Now you are ready to run VQL queries!

## Examples

Show all processes:

```
velociraptor "select * from pslist()"
```
