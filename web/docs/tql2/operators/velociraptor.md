# velociraptor

Submits VQL to a Velociraptor server and returns the response as events.

```tql
velociraptor [request_name=str, org_id=str, max_rows=int,
              subscribe=str, query=str, max_wait=duration, profile=str]
```

## Description

The `velociraptor` source operator provides a request-response interface to a
[Velociraptor](https://docs.velociraptor.app) server:

![Velociraptor](velociraptor.excalidraw.svg)

The pipeline operator is the client and it establishes a connection to a
Velociraptor server. The client request contains a query written in the
[Velociraptor Query Language (VQL)][vql], a SQL-inspired language with a `SELECT
.. FROM .. WHERE` structure.

[vql]: https://docs.velociraptor.app/docs/vql

You can either send a raw VQL query via `velociraptor query "<vql>"` to a
server and processs the response, or hook into a continuous feed of artifacts
via `velociraptor subscribe <artifact>`. Whenever a hunt runs that contains
this artifact, the server will forward it to the pipeline and emit the artifact
payload in the response field `HuntResults`.

All Velociraptor client-to-server communication is mutually authenticated and
encrypted via TLS certificates. This means you must provide client-side
certificate, which you can generate as follows. (Velociraptor ships as a static
binary that we refer to as `velociraptor-binary` here.)

1. Create a server configuration `server.yaml`:
   ```bash
   velociraptor-binary config generate > server.yaml
   ```

2. Create an API client:
   ```bash
   velociraptor-binary -c server.yaml config api_client name tenzir client.yaml
   ```

   Copy the generated `client.yaml` to your Tenzir plugin configuration
   directory as `velociraptor.yaml` so that the operator can find it:
   ```bash
   cp client.yaml /etc/tenzir/plugin/velociraptor.yaml
   ```

3. Run the frontend with the server configuration:
   ```bash
   velociraptor-binary -c server.yaml frontend
   ```

Now you are ready to run VQL queries!

### `request_name = str (optional)`

An identifier for the request to the Velociraptor server.

Defaults to a randoum UUID.

### `org_id = str (optional)`

The ID of the Velociraptor organization.

Defaults to `"root"`.

### `query = str (optional)`

The [VQL][vql] query string.

### `max_rows = int (optional)`

The maxium number of rows to return in a the stream gRPC messages returned by
the server.

Defaults to `1000`.

### `subscribe = str (optional)`

Subscribes to a flow artifact.

This option generates a larger VQL expression under the hood that creates one
event per flow and artifact. The response contains a field `HuntResult` that
contains the result of the hunt.

### `max_wait = duration (optional)`

Controls how long to wait before releasing a partial result set.

Defaults to `1s`.

### `profile = str (optional)`

Specifies the configuration profile for the Velociraptor instance. This enables
connecting to multiple Velociraptor instances from the same Tenzir node.

To use profiles, edit your `velociraptor.yaml` configuration like this, where
`<config>` refers to the contents of the configuration file created by Velociraptor, and
`<profile>` to the desired profile name.

```yaml
# before
<config>

# after
profiles:
  <profile>:
    <config>
```

If profiles are defined, the operator defaults to the first profile.

## Examples

### Show all processes

```tql
velociraptor query="select * from pslist()"
```

### Subscribe to a hunt flow containing the `Windows` artifact

```tql
velociraptor subscribe="Windows"
```
