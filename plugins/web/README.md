# VAST Rest Plugin

This plugin provides a web server and access to the REST frontend for VAST.

The following describes the backend web server part of the web plugin.
For information regarding the frontend VAST UI, see [this](./ui/README.md).

## Usage

To spin up the REST server as a separate process, the `vast web server`
subcommand can be used:

    vast web server --certfile=/path/to/server.certificate --keyfile=/path/to/private.key

By default, the server will only accept TLS requests. To allow clients to
connect succesfully, a valid certificate and the corresponding private key
need to be passed with the `--certfile` and `--keyfile` arguments.

Alternatively, one can run the server within the main VAST process by
specifying a start command:

    vast start --commands="web server [...]"

All requests must be authenticated by a valid authentication token,
which is a short string that must be sent in the `X-VAST-Token` request
header.

A valid token can be generated on the command line using the following command

    vast web generate-token

For local testing and development, generating suitable certificates and tokens
can be a hassle. For this scenario, the server can be started in "development"
mode where plain HTTP connections are accepted and no authentication is performed:

    vast web server --mode=dev

## Security

When providing the API as a network-facing service, it is the plugin enforces the
use of TLS to provide data confidentiality.

### Development Mode

This is suitable for developers who work on VAST and want to test the
API on their local machines. In this mode, VAST accepts plain HTTP connections
and ignores all authentication tokens.

### Server Mode

This is suitable where VAST is bound to an external network interface.
It will accept only HTTPS connections and require valid authentication
tokens for any authenticated endpoints.

This is the default mode.

### TLS Upstream Mode

This is suitable where VAST is configured as the upstream of a separate
TLS terminator that is running on the same machine. This kind of setup
is commonly encountered when running nginx as a reverse proxy.

VAST will only listen on localhost addresses, accept plain HTTP but still
check authentication tokens.

### Mutual TLS Mode

This is suitable where VAST is configured as the upstream of a separate
TLS terminator that may be running on a different machine. This kind of
setup is commonly encountered when running nginx as a load balancer.
Typically VAST would be configured to use a self-signed certificate
in this setup.

VAST will only accept HTTPS requests, require TLS client certificates for
incoming connections, and require valid authentication tokens for any
authenticated endpoints.
