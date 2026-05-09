"""Project hooks for legacy integration tests."""

from tenzir_test import hooks


@hooks.project_start
def disable_neo_executor(ctx):
    # FIXME: This intentionally keeps the lingering legacy tests on the old
    # executor without poisoning tenzir-test's neo-based version probe. Remove
    # this soon, once the remaining tests have moved to `test/` or have been
    # deleted.
    ctx.env["TENZIR_NEO"] = "false"
