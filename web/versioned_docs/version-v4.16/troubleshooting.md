# Troubleshooting

Sometimes things go wrong. Oh no :-(. This page provides guidance on what to do
in error scenarios we've encountered.

:::tip Get help!
Need someone to talk to? Swing by our [Discord](/discord) channel where the
Tenzir team and the community hang out to help each other. Alternatively, send
us an email at support@tenzir.com. We'll help you out as soon as possible.
:::

## Connectivity

### A node does not connect to the platform

After you've followed the instructions to [deploy a
node](setup-guides/deploy-a-node.md), the node does not show up in the platform.

Here's what you can do:

1. Ensure that your firewall allows outbound 443/TCP traffic.
2. Start the node manually on the command line via `tenzir-node` and observe the
   output. In case you see a warning or an error, share it with us.

### A node fails to connect with `system_error: failed to resolve`

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
