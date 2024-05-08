The `geoip` context plugin has a new `load` option, which allows later loading
of mmdb contexts and the creation of empty contexts, e.g.,
`context create empty_context geoip`
`load path/to/mmdb | context load empty_context`
