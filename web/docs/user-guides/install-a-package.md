# Installing packages

A package is a set of pipelines and contexts that thematically belong
together, that is deployed together as one unit.

More specifically, a package definition contains of three major parts:

  1) Package metadata, for example the name, author and
     description of the package.
  2) A set of pipelines, contexts and snippets that make
     up the contents of the package.
  3) A set of inputs that are provided by the user that you
     can use to customize a package to your specific
     environment. Inputs can be optional or required.

Read this [blog post](https://docs.tenzir.com/blog/tenzir-v4.19) for a more
in-depth look at the package definition format, or take look at our
[library packages](https://github.com/tenzir/library) to see some real-world
examples.

Generally, packages are installed by obtaining a valid package
definition and making it available to the node along with all
required inputs. There are currently two main ways to do so.

:::note Coming Soon
The most convenient way to install packages is the Tenzir Library,
which will be available on `app.tenzir.com` in the near future.
Stay tuned!
:::


## Using the package operator

You can install a package interactively by using the
[`package` operator](/operators/package):

```
from file ./package.yaml
| package add
```

If the package contains any required inputs, you need to
ensure their values are set before adding the package.

This is done by opening the `package.yaml` in your favorite
editor and adding a `config` section to the package definition
that contains the necessary values:

```
config:
  inputs:
    foo: Hello,
    bar: World!
```

:::note Coming soon
Using TQL2, it will be much more convenient to set the required inputs
directly in the pipeline that is used to add the package.
Stay tuned!
:::

To uninstall a package installed this way, use the package operator again:

```
package remove <package_id>
```

## From Configuration

A package can be installed by placing a package directory in the node
search path.

A package directory is a directory containing a file called `package.yaml`
that contains a valid package definition. Optionally, a package directory
may also contain a file called `config.yaml`. If it exists, the contents
of `config.yaml` will be inserted into the top-level `config` key of
the package definition.

The node search path for packages consists of the following locations:

 - `<sysconfdir>/tenzir/packages`
 - `~/.config/tenzir/packages`. Tenzir respects Tenzir respects the XDG base
   directory specification and its environment variables.
 - All directories given by `tenzir.package-dirs` configuration variable. The
   value of the `tenzir.package-dirs` variable is determined according to the
   rules described under [configuration](/configuration#configuration-files).

When starting up, the node reads all packages from all package directories
and automatically installs them. Packages installed this way are removed
by removing the corresponding package directory. Note that all changes are
only applied when the node restarts.
