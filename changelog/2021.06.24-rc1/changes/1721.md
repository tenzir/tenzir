VAST now merges lists from configuration files. E.g., running VAST with
`--plugins=some-plugin` and `vast.plugins: [other-plugin]` in the
configuration now results in `some-plugin` and `other-plugin` being
loaded (sorted by the usual precedence).
