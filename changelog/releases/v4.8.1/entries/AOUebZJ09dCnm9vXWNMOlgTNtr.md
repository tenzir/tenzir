---
title: "Enable the AMQP plugin in the static binary"
type: bugfix
author: dominiklohmann
created: 2024-01-23T13:17:15Z
pr: 3854
---

The `amqp` connector plugin was incorrectly packaged and unavailable in some
build configurations. The plugin is now available in all builds.

Failing to create the virtualenv of the `python` operator caused subsequent uses
of the `python` operator to silently fail. This no longer happens.

The Debian package now depends on `python3-venv`, which is required for the
`python` operator to create its virtualenv.
