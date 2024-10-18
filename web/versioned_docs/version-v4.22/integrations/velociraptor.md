# Velociraptor

[Velociraptor](https://docs.velociraptor.app) is a digital forensics and
incident response (DFIR) tool for interrogating endpoints.

Use Tenzir to conveniently speak with a Velociraptor server over the [gRPC
API][api].

[api]: https://docs.velociraptor.app/docs/server_automation/server_api/

![Velociraptor](../operators/velociraptor.excalidraw.svg)

## Create a TLS certificate to communicate with Velociraptor

The `velociraptor` acts as client and establishes a connection to a Velociraptor
server via gRPC. All Velociraptor client-to-server communication is mutually
authenticated and encrypted via TLS certificates. This means you must provide a
client-side certificate, which you can generate as follows. (Velociraptor ships
as a static binary that we refer to as `velociraptor-binary` here.)

1. Create a server configuration `server.yaml`:
   ```bash
   velociraptor-binary config generate > server.yaml
   ```

2. Create an API client:
   ```bash
   velociraptor-binary -c server.yaml config api_client --name tenzir client.yaml
   ```

   Copy the generated `client.yaml` to your Tenzir [plugin
   configuration](../configuration.md#plugins) directory as `velociraptor.yaml`
   so that the operator can find it:
   ```bash
   cp client.yaml /etc/tenzir/plugin/velociraptor.yaml
   ```

3. Create a user (e.g., an admin named `tenzir`):
   ```bash
   velociraptor-binary -v -c server.yaml user add --role administrator tenzir
   ```

4. Run the frontend with the server configuration:
   ```bash
   velociraptor-binary -c server.yaml frontend
   ```

## Run raw VQL

After you have created a TLS certificate, you can use the `velociraptor`
operator to execute a [Velociraptor Query Language
(VQL)](https://docs.velociraptor.app/docs/vql/) query:

```
velociraptor --query "select * from pslist()"
| select Name, Pid, PPid, CommandLine
| where Name == "remotemanagement"
```

## Subscribe to forensic artifacts

You can also hunt for forensic artifacts, such as dropped files or specific
entries in the Windows registry, on assets connected to your Velociraptor
server. Every time a client reports back on an artifact that matches a given
Regex, e.g., `Windows` or `Windows.Sys.StartupItems`, the Velociraptor server
sends the result into the pipeline.

For example, run this pipeline to subscribe to an artifact collection of Windows
startup items and import them into a node:

```
velociraptor --subscribe Windows.Sys.StartupItems | import
```

For additional information, read our blog post [Integrating Velociraptor into
Tenzir Pipelines](/blog/integrating-velociraptor-into-tenzir-pipelines).
