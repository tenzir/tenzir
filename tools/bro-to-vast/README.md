# bro-to-vast

The **bro-to-vast** tool enables [Bro](https://bro.org) to speak with
[VAST](http://vast.io) via [Broker](https://github.com/bro/broker). That is,
it acts as bridge between Bro and VAST:

                  Bro  <--->  bro-to-vast  <--->  VAST

The accompanying Bro code ships as the separate package [bro-vast][bro-vast].

## Installation

First, make sure you have Bro and Broker installed. VAST requires Broker at
build time to create the `bro-to-vast` binary.

Second, install the Bro scripts via:

    bro-pkg install bro-vast

Now you're ready to go.

## Usage

VAST must be running prior to starting `bro-to-vast`. If VAST listens on the
default port (42000), then you only need to invoke the binary:

    bro-to-vast

Otherwise, you can provide a different port with the `-p` flag:

    bro-to-vast -p 44444

After `bro-to-vast` connected to VAST successfully, connection attempts from
Bro can succeed.

The `bro-to-vast` bridge acts primarily as a "dumb" relay and does not contain
much logic. Please consult the documentation of the Bro package
[bro-vast][bro-vast] for concrete Bro use cases.

[bro-vast]: https://github.com/tenzir/bro-vast
