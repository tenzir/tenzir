# zeek-to-vast

The **zeek-to-vast** tool enables [Zeek](https://zeek.org) to speak with
[VAST](http://vast.io) via [Broker](https://github.com/zeek/broker). That is,
it acts as bridge between Zeek and VAST:

                  Zeek  <--->  zeek-to-vast  <--->  VAST

Note that `zeek-to-vast` acts as a "dumb" relay and does not contain much logic.
Please consult the documentation of the Zeek package [zeek-vast][zeek-vast] for
concrete Zeek use cases.

## Installation

First, make sure you have Zeek and Broker installed. VAST automatically builds
`zeek-to-vast` if Broker is found during the build configuration. If you're
using the `configure` script in this repo, the flag `--with-broker=PATH` allows
for specifying a custom install location. `PATH` is either an install prefix or
a build directory of a Broker repository.

Second, install the Zeek scripts via:

    zkg install zeek-vast

Now you're ready to go.

## Usage

As illustrated above, `zeek-to-vast` sits between Zeek and VAST. If Zeek and
VAST run on the same machine, all you need to do is invoke the program:

    zeek-to-vast

VAST must be running prior to invocation. After connecting to VAST
successfully, `zeek-to-vast` creates a Broker endpoint and waits for Zeek to
connect.

Both sides can be configured separately. To configure the VAST-facing side, use
the options `--vast-address` (or `-A`) and `--vast-port` (or `-P`):

    # Connect to a VAST node running at 10.0.0.1:55555.
    zeek-to-vast -A 10.0.0.1 -P 55555

To configure the Zeek-facing side, use the options `--broker-address` (or `-a`)
and `--broker-port` (or `-p`):

    # Wait for Zeek to connect at 192.168.0.1:44444
    zeek-to-vast -a 192.168.0.1 -p 44444

[zeek-vast]: https://github.com/tenzir/zeek-vast
