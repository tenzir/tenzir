The 'export' parameter for '/export' family of endpoints is renamed to 'query'.
The 'query' parameter accepts an optional 'pipeline' string e.g.
:ip in 10.42.0.0/16 | head 50. This causes the pipeline operators to be applied
onto the exported data.
