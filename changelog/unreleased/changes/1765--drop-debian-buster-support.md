VAST no longer officially supports Debian Buster with GCC-8 and is instead now
being tested on Debian Bullseye with GCC-10. The provided Docker images now use
`debian:buster-slim` as their base image. We recommend users that require Debian
Buster support to use the provided static Nix builds instead.
