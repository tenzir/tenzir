# VAST Rest Plugin

This plugin provides a REST frontend for VAST. It can be run either

# Security

We provide presets for four common deployment scenarios via the `--mode`
parameter of VAST:

## Development Mode

This is suitable for developers who work on VAST and want to test the
API on their local machines. In this mode, VAST accepts plain HTTP connections
and ignores all authentication tokens.

    /-------\ 
    | VAST  |<-\
    |       |  |
    |  User |--/
    \-------/


## Server Mode

This is suitable where VAST is bound to an external network interface.
It will accept only HTTPS connections and require valid authentication
tokens for any authenticated endpoints.

This is the default mode.


## TLS Upstream Mode

This is suitable where VAST is configured as the upstream of a separate
TLS terminator that is running on the same machine. This kind of setup
is commonly encountered when running nginx as a reverse proxy.

VAST will only listen on localhost addresses, accept plain HTTP but still
check authentication tokens.

## Mutual TLS Mode

This is suitable where VAST is configured as the upstream of a separate
TLS terminator that may be running on a different machine. This kind of
setup is commonly encountered when running nginx as a load balancer.
Typically VAST would be configured to use a self-signed certificate
in this setup.

VAST will only accept HTTPS requests, require TLS client certificates for
incoming connections, and require valid authentication tokens for any
authenticated endpoints.
