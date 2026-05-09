"""Project hooks for legacy integration tests."""

from tenzir_test import hooks


@hooks.startup
def disable_neo_executor(ctx):
    ctx.env.setdefault("TENZIR_NEO", "false")
