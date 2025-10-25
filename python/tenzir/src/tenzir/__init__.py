_PIPELINE_EXPORTS = {
    "AsyncRecordBatchStreamReader",
    "CompletedPipeline",
    "PipelineError",
    "PipelineIO",
    "PipelineOptions",
    "PipelineRun",
    "PipelineSpec",
    "PyArrowTableSlice",
    "TableSlice",
    "run_pipeline",
    "run_pipeline_sync",
    "stream_pipeline",
}
_CONVERT_EXPORTS = {
    "TenzirRow",
    "arrow_dict_to_json_dict",
    "collect_pyarrow",
    "to_json_rows",
}

__all__ = sorted(_PIPELINE_EXPORTS | _CONVERT_EXPORTS)


def __getattr__(name):
    if name in _PIPELINE_EXPORTS:
        from .tenzir import tenzir as _pipeline_mod

        return getattr(_pipeline_mod, name)
    if name in _CONVERT_EXPORTS:
        from .tenzir import convert as _convert_mod

        return getattr(_convert_mod, name)
    raise AttributeError(name)
