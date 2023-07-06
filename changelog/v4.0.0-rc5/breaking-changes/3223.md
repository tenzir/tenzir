We changed the default connector of `read <format>` and `write <format>` for
all formats to `stdin` and `stdout`, respectively.

We removed language plugins in favor of operator-based integrations.

The interface of the operator, loader, parser, printer and saver plugins was
changed.
