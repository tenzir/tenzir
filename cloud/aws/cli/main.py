from invoke import Program, Collection
import sys
import core
import plugins
import pkgutil


def unhandled_exception(type, value, traceback):
    """Override for `sys.excepthook` without stack trace"""
    print(f"{type.__name__}: {str(value)}")


if __name__ == "__main__":
    sys.excepthook = unhandled_exception

    namespace = Collection.from_module(core)

    # import all the modules in the plugins folder as collections
    for importer, modname, ispkg in pkgutil.iter_modules(plugins.__path__):
        mod = importer.find_module(modname).load_module(modname)
        plugin = Collection.from_module(mod)
        try:
            plugin.configure(mod.INVOKE_CONFIG)
        except:
            pass
        namespace.add_collection(plugin)

    program = Program(
        binary="./vast-cloud",
        namespace=namespace,
        version="0.1.0",
    )
    program.run()
