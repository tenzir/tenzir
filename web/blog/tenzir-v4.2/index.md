---
title: Tenzir v4.2
authors: [dakostu, mavam]
date: 2023-09-07
tags: [release, pipelines, connectors, s3, zmq]
draft: true
---

We've just released Tenzir v4.2 that introduces two new connectors: [S3][s3] for
interacting with blob storage and [ZeroMQ][zeromq] for writing distributed
multi-hop pipelines.

[s3]: https://aws.amazon.com/s3/
[marketplace]: https://aws.amazon.com/marketplace/search/results?trk=8384929b-0eb1-4af3-8996-07aa409646bc&sc_channel=el&FULFILLMENT_OPTION_TYPE=DATA_EXCHANGE&CONTRACT_TYPE=OPEN_DATA_LICENSES&DATA_AVAILABLE_THROUGH=S3_OBJECTS&PRICING_MODEL=FREE&filters=FULFILLMENT_OPTION_TYPE%2CCONTRACT_TYPE%2CDATA_AVAILABLE_THROUGH%2CPRICING_MODEL
[density]: https://aws.amazon.com/marketplace/pp/prodview-jf2hjpr2mrj4m?sr=0-2&ref_=beagle&applicationId=AWSMPContessa#overview
[humor]: https://aws.amazon.com/marketplace/pp/prodview-b53zm25dl3jcc?sr=0-3&ref_=beagle&applicationId=AWSMPContessa#overview
[uri]: https://arrow.apache.org/docs/10.0/r/articles/fs.html#uri-options
[zeromq]: https://zeromq.org/

<!--truncate-->

## S3 Saver & Loader

The new [`s3`](connectors/s3) connector hooks up Tenzir to the vast data masses
on [Amazon S3](https://aws.amazon.com/s3/) and S3-compatible object storage
systems.
We are using Arrow's filesystem abstraction for establishing connections to
these systems. This abstraction already handles AWS's default credentials
provider chain. If you have set up your AWS account in this chain, then you
don't need to worry about setting it up again in config files or similar
formats.

With the `s3` loader, you can access objects on S3 buckets, assuming you have
the proper credentials:

```bash
tenzir 'from s3 s3://bucket/mystuff/file.json'
```

S3 buckets can also be public, meaning you don't need any specific credentials
to access the objects therein.
AWS offers tons of such public (read-only) buckets with scientific data on
their [Marketplace][marketplace].
Tenzir, of course, can also import public read-only data - for example, some
[population density & demographic estimate data][density]:

```bash
tenzir "load s3
s3://dataforgood-fb-data/csv/month=2019-06/country=VGB/type=children_under_five/
VGB_children_under_five.csv.gz | decompress gzip | read csv"
```

Now, let's combine this with `aws s3 ls` to receive all [Amazon product Q&A
humor detection data][humor]:

```bash
aws s3 ls --no-sign-request --recursive s3://humor-detection-pds/ | awk '{print
$4}' | grep "\.csv" | xargs -I {} tenzir "from s3 s3://humor-detection-pds/{}
read csv"
```

The original .csv data is a bit unpolished (for example, there are line breaks
and superfluous commas in the middle of some values) - Tenzir's `csv` parser
will ignore those lines, and the rest of the data will be there at your
fingertips.

The `s3` writer uploads the pipeline output to an object in the bucket:

```bash
tenzir "export | to s3 s3://mybucket/folder/ok.json"
```

Arrow has URI option capabilities, as [this blog post][uri] describes in
detail. The most relevant info for the S3 connector:

> For S3, the options that can be included in the URI as query parameters are
`region`, `scheme`, `endpoint_override`, `access_key`, `secret_key`,
`allow_bucket_creation`, and `allow_bucket_deletion`.

The most exciting of these options would be `endpoint_override` - it allows you
to connect to different endpoints of other, S3-compatible storage systems:

```
from s3
s3://examplebucket/test.json?endpoint_override=s3.us-west.mycloudservice.com
```

The `s3` connector is a huge step for Tenzir's data accessibility. It already
covers a lot of ground, but it is the first of many connectors that will
cover a lot more: More connections, more data, more information, more value.

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

## Concatenating PCAPs

The [`pcap`](/formats/pcap) parser can now read concatenated PCAP files,
allowing you to easily process large amounts of trace files. This comes
especially handy on the command line:

```bash
cat *.pcap | tenzir 'read pcap'
```

The [`nic`](/connectors/nic) loader has a new flag `--emit-file-headers` that
prepends a PCAP file header for every batch of bytes that it produces, yielding
a stream of concatenated PCAP files. This gives rise to creative use cases
involving packet shipping. For example, to ship blocks of packets as "micro
traces" via 0mq, you could do:

```
load nic eth0
| save zmq
```

This creates 0mq PUB socket where subscribes can come and go. Each 0mq message
is a self-contained PCAP trace, which avoids painful resynchronization logic.
You can consume this feed with a remote subscriber:

```
load zmq
| read pcap
```

Finally, we also made it easier to identify available network interfaces when
using the `nic` loader: `show nics` now returns a list of available interfaces.
