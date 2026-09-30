"""Pass resolved package paths to compiler diff runners."""

from tenzir_test import fixture
from tenzir_test.fixtures import current_context


@fixture
def packages() -> dict[str, str]:
    directories = current_context().config["package_dirs"]
    return {"TENZIR_PACKAGE_DIRS": ",".join(directories)}
