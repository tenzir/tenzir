# Install a Package

A [package](../packages.md) bundles pipelines and contexts, making it easy to
deploy them as a single unit.

## Install from the Tenzir Library

:::info Coming Soon
The most convenient way to install a package is through the Tenzir Library,
which will be available on [app.tenzir.com](https://app.tenzir.com) in the near
future. Stay tuned!
:::

## Install with the Package Operator

To install a package interactively, use the [`package add`
operator](../operators/package.md):

```
from ./package.yaml
| package add
```

To set package inputs, simply set the values in the pipeline:

```
from ./package.yaml
| python '
  self.config.inputs.endpoint = "localhost:42000"
  self.config.inputs.policy = "block"
'
| package add
```

Your package should now show when listing all installed packages:

```
show packages
```

```json5
{
  "id": "your-package",
  "install_status": "installed",
  // …
}
```

To uninstall a package interactively, use the [`package remove`
operator](../operators/package.md).

```
package remove your-package
```

## Install with Infrastructure as Code

The node searches for packages in a set of directories on startup. Packages live
right next to your `tenzir.yaml` file:

```
/opt/tenzir/etc/tenzir
├── tenzir.yaml
└── packages
    └── your-package
        ├── config.yaml
        └── package.yaml
```

Inside the `packages` directory, every package lives in its own directory (which
by convention should have a name matching the package id), containing a
`package.yaml` file with the package definition.

The node search path for packages consists of the following locations:
1. The `packages` directory in all [configuration
   directories](../configuration.md#configuration-files).
2. All directories specified in the `tenzir.package-dirs` configuration option.

The directory may optionally contain a `config.yaml` file for customizing the
package installation:

```yaml
inputs:
  endpoint: localhost:42000
  policy: block
```
