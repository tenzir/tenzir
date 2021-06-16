Plugins now load their respective configuration from
`<configdir>/vast/plugin/<plugin-name>.yaml` in addition to the regular
configuration file at `<configdir>/vast/vast.yaml`. The new plugin-specific file
does not require putting configuration under the key `plugins.<plugin-name>`.
This allows for deploying plugins without needing to touch the
`<configdir>/vast/vast.yaml` configuration file.
