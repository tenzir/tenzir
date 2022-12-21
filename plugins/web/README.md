# VAST Web Plugin

The `web` plugin provides a web server that serves both the REST API as well as
the web frontend.

For detailed usage instructions of the REST API, please consult the
[documentation][rest-api]. The frontend has its own README in the [ui](ui)
subdirectory.

[rest-api]: https://vast.io/docs/use/integrate/rest-api

## Usage

To run the REST API as dedicated process, use the `web server` command:

```bash
vast web server --certfile=/path/to/server.certificate --keyfile=/path/to/private.key
```
To run the server within the main VAST process, use a `start` command:

```bash
vast start --commands="web server [...]"
```

All requests must be authenticated by a valid authentication token, which is a
short string that must be sent in the `X-VAST-Token` request header.

You can generate a valid token on the command line as follows:

```bash
vast web generate-token
```

Use the "developer mode" to bypass encryption and token validation:

```bash
vast web server --mode=dev
```
