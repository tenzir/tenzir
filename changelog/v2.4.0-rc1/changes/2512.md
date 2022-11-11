We changed the default VAST endpoint from 'localhost'
to '127.0.0.1', to ensure the listen address is deterministic
and eliminate a race where VAST fails to acquire the IPv6
address due to a lingering port reservation but then successfully
listens on the IPv6 address.
