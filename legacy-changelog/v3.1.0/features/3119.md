Pipelines may now span across multiple processes. This will enable upcoming
operators that do not just run locally in the `vast exec` process, but rather
connect to a VAST node and partially run in that node. The new operator
modifiers `remote` and `local` allow expert users to control where parts of
their pipeline run explicitly, e.g., to offload compute to a more powerful node.
Potentially unsafe use of these modifiers requires setting
`vast.allow-unsafe-pipelines` to `true` in the configuration file.
