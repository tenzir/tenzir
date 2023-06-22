The pipeline manager now accepts empty strings for the optional `name`. The
`/create` endpoint returns a list of diagnostics if pipeline creation fails,
and if `start_when_created` is set, the endpoint now returns only after the
pipeline execution has been fully started. The `/list` endpoint now returns
the diagnostics collected for every pipeline so far. The `/delete` endpoint
now returns an empty object if the request is successful.
