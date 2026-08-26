---
title: Nix-built container images
type: change
authors:
  - tobim
created: 2026-08-22T00:00:00.000000Z
---

The `tenzir/tenzir`, `tenzir/tenzir-node`, and `tenzir/tenzir-demo` images are
now built from the same Nix expression as the `-slim` images instead of from a
Debian-based Dockerfile. The images keep their runtime contract - the
unprivileged `tenzir` user with uid and gid 999, the state, cache, and log
directories under `/var`, `TENZIR_ENDPOINT=0.0.0.0`, and the `/var/lib/tenzir`
working directory - but their dependency closure and their reported build
metadata now match the `-slim` images.

All images read configuration from `/opt/tenzir/etc/tenzir`, the location the
Debian-based images used, so a `tenzir.yaml` or a `packages/` directory mounted
there is picked up. The Nix-built images previously derived that path from their
Nix store prefix, where nothing can be mounted, and silently used no
configuration at all.

The `-slim` images gain that runtime contract in return: they no longer run as
`root`, they ship pre-created state directories, `/tmp` is writable, and HTTPS
works - their trust store was installed under a name the HTTP client does not
look for, so every TLS connection failed to load its CA paths.

The `tenzir/tenzir-demo` image works again. It now ships the demo package and
two pre-configured pipelines that import the Zeek and Suricata demo feeds when
the node starts. Previously the image tried to install a package that no longer
exists in the library, so a demo node came up without data.

All images are roughly 700 MB smaller. Their bundled Python runtime environment
no longer contains an unused Ansible installation that a packaging mistake had
pulled in.

The `tenzir/tenzir-deps` image is no longer published. It only ever carried the
Debian build dependencies, which the Nix build does not use.
