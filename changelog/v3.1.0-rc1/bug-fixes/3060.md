VAST incorrectly handled subnets using IPv6 addresses for which an equivalent
IPv4 address existed. This is now done correctly. For example, the query `where
:ip !in ::ffff:0:0/96` now returns all events containing an IP address that
cannot be represented as an IPv4 address. As an additional safeguard, the VAST
language no longer allows for constructing subnets for IPv4 addresses with
lengths greater than 32.
