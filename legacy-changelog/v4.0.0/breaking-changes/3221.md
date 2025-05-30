We removed the `rest_endpoint_plugin::prefix()` function from
the public API of the `rest_endpoint_plugin` class. For a migration,
existing users should prepend the prefix manually to all endpoints
defined by their plugin.
