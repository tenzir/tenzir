---
title: "Bump minimum Debian requirement to Bullseye"
type: change
authors: dominiklohmann
pr: 1765
---

VAST no longer officially supports Debian Buster with GCC-8. In CI, VAST now
runs on Debian Bullseye with GCC-10. The provided Docker images now use
`debian:bullseye-slim` as base image. Users that require Debian
Buster support should use the provided static builds instead.
