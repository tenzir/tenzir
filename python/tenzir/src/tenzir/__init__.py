_TENZIR_EXPORTS = {"Tenzir", "ExportMode", "TableSlice"}
_CONVERT_EXPORTS = {
    "collect_pyarrow",
    "to_json_rows",
    "VastRow",
    "arrow_dict_to_json_dict",
}

__all__ = sorted(_TENZIR_EXPORTS | _CONVERT_EXPORTS)


def __getattr__(name):
    if name in _TENZIR_EXPORTS:
        from .tenzir import tenzir as _tenzir_mod

        return getattr(_tenzir_mod, name)
    if name in _CONVERT_EXPORTS:
        from .tenzir import convert as _convert_mod

        return getattr(_convert_mod, name)
    raise AttributeError(name)
