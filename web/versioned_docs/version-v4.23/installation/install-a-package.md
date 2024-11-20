# Install a Package

A [package](../packages.md) bundles pipelines and contexts, making it easy to
deploy them as a single unit.

## Install from the Tenzir Library

The most convenient way to install a package is through the [Tenzir
Library](https://app.tenzir.com/library):

1. Click on a package
2. Select the *Install* tab
3. Define your inputs (optional)
4. Click the *Install* button in the bottom right

## Install with the Package Operator

To install a package interactively, use the [`package_add`
operator](../operators/package.md):

```tql
// tql2
package_add "package.yaml"
```

To set package inputs, set the values in the pipeline:

```tql
// tql2
package_add "package.yaml", inputs={
  endpoint: "localhost:42000",
  policy: "block",
}
```

Your package should now show when listing all installed packages:

```tql
packages
```

```json5
{
  "id": "your-package",
  "install_status": "installed",
  // …
}
```

To uninstall a package interactively, use
[`package_remove`](../operators/package.md).

```tql
package_remove "your-package"
```

## Install with Infrastructure as Code

For IaC-style deployments, you can install packages *as code* by putting them
next to your `tenzir.yaml` configuration file:

```
/opt/tenzir/etc/tenzir
├── tenzir.yaml
└── packages
    └── your-package
        ├── config.yaml
        └── package.yaml
```

Inside the `packages` directory, every installed package lives in its own
directory containing a `package.yaml` file with the package definition. By
convention, the directory name is the package ID.

The node search path for packages consists of the following locations:

1. The `packages` directory in all [configuration
   directories](../configuration.md#configuration-files).
2. All directories specified in the `tenzir.package-dirs` configuration option.

As an alternative way to specify inputs visually in the app, or setting them
explicitly as part of calling `package_add`, you can add a `config.yaml` file
next to the `package.yaml` file. Here is an example that sets the inputs
`endpoint` and `policy`:

```yaml
inputs:
  endpoint: localhost:42000
  policy: block
```
