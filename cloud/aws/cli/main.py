from invoke import Program, Collection
import sys
import core
import common
import plugins
import pkgutil
import flags


def unhandled_exception(type, value, traceback):
    """Override for `sys.excepthook` only showing stack trace if requested"""
    if flags.TRACE:
        sys.__excepthook__(type, value, traceback)
    else:
        print(f"{type.__name__}: {str(value)}", file=sys.stderr)


class VastCloudProgram(Program):
    """A custom Program that doesn't print useless core options"""

    def print_help(self) -> None:
        print(
            f"""Usage: {self.binary} [--core-opts] <subcommand> [--subcommand-opts] ...

Core options:

  -e, --echo                     Echo executed commands before running.
  -h [STRING], --help[=STRING]   Show core or per-task help and exit.
  -V, --version                  Show version and exit.

Supported plugins: {", ".join([mod_tup[1] for mod_tup in pkgutil.iter_modules(plugins.__path__)])}
Active plugins: {", ".join(common.active_plugins())}
"""
        )
        self.list_tasks()


if __name__ == "__main__":
    sys.excepthook = unhandled_exception

    namespace = Collection.from_module(core)
    namespace.configure({"run": {"env": common.conf(core.VALIDATORS)}})

    plugin_set = common.active_plugins()
    # import all the modules in the plugins folder as collections
    for importer, modname, ispkg in pkgutil.iter_modules(plugins.__path__):
        if modname in plugin_set:
            mod = importer.find_module(modname).load_module(modname)
            namespace.add_collection(Collection.from_module(mod))
            if hasattr(mod, "VALIDATORS"):
                namespace.configure({"run": {"env": common.conf(mod.VALIDATORS)}})
            plugin_set.remove(modname)

    if len(plugin_set) > 0:
        sys.exit(f"Unkown plugins: {plugin_set}")

    program = VastCloudProgram(
        binary="./vast-cloud",
        namespace=namespace,
        version="0.1.0",
    )
    program.run()
