Static plugins are no longer always loaded, but rather need to be explicitly
enabled as documented. To restore the behavior from before this bug fix, set
`vast.plugins: [bundled]` in your configuration file.
