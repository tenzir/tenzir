We changed VAST client processes to attempt connecting to a VAST server
multiple times until the configured connection timeout (vast.connection-timeout
, defaults to 5 minutes) runs out. A fixed delay between connection attempts
(vast.connection-retry-delay, defaults to 3 seconds) ensures that clients to
not stress the server too much. Set the connection timeout to zero to let VAST
client attempt connecting indefinitely, and the delay to zero to disable the
retry mechanism.
