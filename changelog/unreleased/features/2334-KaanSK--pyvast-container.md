PyVAST now supports running client commands for VAST servers running in a
container environment, if no local VAST binary is available. Specify the
`container` keyword to customize this behavior. It defaults to `{"runtime":
"docker", "name": "vast"}`.
