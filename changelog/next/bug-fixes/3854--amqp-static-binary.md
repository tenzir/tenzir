The `amqp` connector plugin was incorrectly packaged and unavailable in some
build configurations. The plugin is now available in all builds.

Failing to create the virtualenv of the `python` operator caused subsequent uses
of the `python` operator to silently fail. This no longer happens.
