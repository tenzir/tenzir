# bro-to-vast

The **bro-to-vast** tool enables [Bro](https://bro.org) to speak with
[VAST](http://vast.io) via [Broker](https://github.com/bro/broker). That is,
it acts as bridge between Bro and VAST:

                  Bro  <--->  bro-to-vast  <--->  VAST

Note that `bro-to-vast` acts as a "dumb" relay and does not contain much logic.
Please consult the documentation of the Bro package [bro-vast][bro-vast] for
concrete Bro use cases.

## Installation

First, make sure you have Bro and Broker installed. VAST automatically builds
`bro-to-vast` if Broker is found during the build configuration. If you're
using the `configure` script in this repo, the flag `--with-broker=PATH` allows
for specifying a custom install location. `PATH` is either an install prefix or
a build directory of a Broker repository.

Second, install the Bro scripts via:

    bro-pkg install bro-vast

Now you're ready to go.

## Usage

As illustrated above, `bro-to-vast` sits between Bro and VAST. If Bro and VAST
run on the same machine, all you need to do is invoke the program:

    bro-to-vast

VAST must be running prior to invocation. After connecting to VAST
successfully, `bro-to-vast` creates a Broker endpoint and waits for Bro to
connect.

Both sides can be configured separately. To configure the VAST-facing side, use
the options `--vast-address` (or `-A`) and `--vast-port` (or `-P`):

    # Connect to a VAST node running at 10.0.0.1:55555.
    bro-to-vast -A 10.0.0.1 -P 55555

To configure the Bro-facing side, use the options `--broker-address` (or `-a`)
and `--broker-port` (or `-p`):

    # Wait for Bro to connect at 192.168.0.1:44444
    bro-to-vast -a 192.168.0.1 -p 44444

[bro-vast]: https://github.com/tenzir/bro-vast
