---
title: Tenzir v4.2
authors: [dakostu, mavam]
date: 2023-09-07
tags: [release, pipelines, connectors, s3, zmq]
draft: true
---

We've just released Tenzir v4.2 that introduces two new connectors: S3 for
interacting with blob storage and [ZeroMQ][zeromq] for writing distributed
multi-hop pipelines.

[zeromq]: https://zeromq.org/

<!--truncate-->

## S3 Saver & Loader

TODO

## ZeroMQ Saver & Loader

The new [`zmq`](/connectors/zmq) connector makes it easy to interact with the
raw bytes in [ZeroMQ][zeromq] messages. We model the `zmq` *loader* as subscriber with a `SUB` socket, and the *saver* as a publisher with the `PUB` socket:

![ZeroMQ Connector](zeromq-connector.excalidraw.svg)

What's nice about ZeroMQ is that the directionality of connection establishment
is independent of the socket type. So either end can bind or connect. We opted
for the subscriber to connect by default, and the publisher to bind. You can
override this with the `--bind` and `--connect` flags.

Even though we're using a lossy `PUB`-`SUB` socket pair, we've added a thin
layer of reliability in that a Tenzir pipeline won't send or receive ZeroMQ
messages before it has at least one connected socket.

Want to exchange and convert events with two single commands? Here's how you
publish JSON and continue as CSV on the other end:

```bash
# Publish some data via a ZeroMQ PUB socket:
tenzir 'show operators | to zmq write json'
# Subscribe to it in another process
tenzir 'from zmq read json | write csv'
```

You can also work with operators that use types. Want to send away chunks of
network packets to a remote machine? Here you go:

```bash
# Publish raw bytes:
tenzir 'load nic eth0 | save zmq'
# Tap into the raw feed at the other end and start parsing:
tenzir 'load zmq | read pcap | decapsulate'
```

Need to expose the source side of a pipeline as a listening instead of
connecting socket? No problem:

```bash
# Bind instead of connect with the ZeroMQ SUB socket:
tenzir 'from zmq --bind'
```

These examples show the power of composability: Tenzir operators work with
both bytes and events, enabling in-flight reshaping, format conversation, or
simply data shipping at ease.

## HTTP and FTP Loader

We've added a new round of loaders for HTTP and FTP, named `http`, `https`,
`ftp`, and `ftps`. This makes it a lot easier to pull data into a pipeline that
lives at a remote web or file server. No more `shell curl` shenanigans!

We modeled the `http` and `https` loaders after [HTTPie](https://httpie.io/),
which comes with an expressive and intuitive command-line syntax. We recommend
to study the [HTTPie documentation](https://httpie.io/docs/cli/examples) to
understand the full extent of the command-line interface. In many cases, you can
perform an *exact* copy of the HTTPie command line and use it drop-in with the
HTTP loader, e.g., the invocation

```bash
http PUT pie.dev/put X-API-Token:123 foo=bar
```

becomes

```
from http PUT pie.dev/put X-API-Token:123 foo=bar
```

More generally, if your HTTPie command line is `http X` then you can write `from
http X` to obtain an event stream or `load http X` for a byte stream. (Note that
we have only the parts of the HTTPie syntax most relevant to our users.)

Internally, we rely on [libcurl](https://curl.se/libcurl/) to perform the actual
file transfer. It is noteworthy that libcurl supports *a lot* of protocols:

> libcurl is a free and easy-to-use client-side URL transfer library, supporting
> DICT, FILE, FTP, FTPS, GOPHER, GOPHERS, HTTP, HTTPS, IMAP, IMAPS, LDAP, LDAPS,
> MQTT, POP3, POP3S, RTMP, RTMPS, RTSP, SCP, SFTP, SMB, SMBS, SMTP, SMTPS,
> TELNET and TFTP. libcurl supports SSL certificates, HTTP POST, HTTP PUT, FTP
> uploading, HTTP form based upload, proxies, HTTP/2, HTTP/3, cookies,
> user+password authentication (Basic, Digest, NTLM, Negotiate, Kerberos), file
> transfer resume, http proxy tunneling and more!

[Let us know](/discord) if you have use cases for any of these. Let's take a
look at some more that you can readily work with.

Download and process a [CSV](/formats/csv) file:

```
from http example.org/file.csv read csv
```

Process a Zstd-compressed [Zeek TSV](/formats/zeek-tsv) file:

```
load http example.org/gigantic.log.zst
| decompress zstd
| read zeek-tsv
```

Import a [CEF](/formats/cef) log from an FTP server into a Tenzir node:

```
load ftp example.org/cef.log read cef
| import
```

## Lines Parser

The new [`lines`](/formats/lines) parser splits its input at newline characters
and produces events with a single field representing the line. This parser is
especially useful for onboarding line-based text files into pipelines.

The `-s|--skip-empty` flags ignores empty lines. For example, read a text file
as follows:

```
from file /tmp/test.txt read lines --skip-empty
```
