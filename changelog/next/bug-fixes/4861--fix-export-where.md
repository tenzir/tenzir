Pipelines that begin with `export | where` followed by an expression that does
not depend on the incoming events, such as `export | where 1 == 1`, no longer
cause an internal error.
