# Troubleshooting

This page provides guidance when things go wrong.

## Common Issues

### Node fails to connect with `system_error: failed to resolve`

You may see this error message when a node attempts to connect to the platform:

```
platform-client failed to connect: !! system_error: failed to connect: !! system_error: failed to resolve; will retry in 2m
```

This can happen when additional name servers for custom domains are configured
in your `/etc/resolv.conf`. This is commonly referred to as [Split
DNS](https://en.wikipedia.org/wiki/Split-horizon_DNS). The name resolution
algorithm in our official Linux binaries does not support such a setup natively.
A split DNS setup is best implemented by using a local caching nameserver such
as [dnsmasq](https://thekelleys.org.uk/dnsmasq/doc.html) or
[systemd-resolved](https://systemd.io/RESOLVED-VPNS/).
