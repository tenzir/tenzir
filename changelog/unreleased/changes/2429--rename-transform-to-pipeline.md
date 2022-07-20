Transform has been renamed to pipeline. This mandates a change in the yaml
configuration options:
- `transform-triggers` -> `pipeline_triggers`
- `transforms` -> `pipelines`
- `transform` -> `pipeline`

For custom transform plugins, the plugin base class has been changed
from `transform_plugin` to `pipeline_operator_plugin`.
