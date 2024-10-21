#!/usr/bin/env python3

from pathlib import Path
import sys
import textwrap
import typing

import requests

ALL = object()

# ================== Configuration ================== #
URL = "https://schema.ocsf.io/export/schema"
DOCUMENT_ENTITIES = True
DOCUMENT_FIELDS = False
CLASS_PREFIX = "ocsf."
OBJECT_PREFIX = "ocsf.object."
COLUMN_LIMIT = 80
PROFILES = ALL  # List of strings or ALL.
# =================================================== #

OMIT_MARKER = object()
BASIC_TYPES = {
    "boolean_t": "bool",
    "float_t": "double",
    "integer_t": "int64",
    # TODO: Don't omit this.
    "json_t": OMIT_MARKER,
    "long_t": "int64",
    "string_t": "string",
    "bytestring_t": "blob",
    "datetime_t": "time",
    "ip_t": "ip",
    "subnet_t": "subnet",
    # TODO: Is this the best choice?
    "timestamp_t": "time",
}

# TODO: The schema defines some recursive types which cannot be represented
# faithfully. Hence, we omit some fields to break recursive cycles. Instead of
# that, we could recursively unfold the types up to a certain depth, or perhaps
# use a slightly different representation.
OMIT = {
    "ldap_person": ["manager"],
    "process": ["parent_process"],
    "network_proxy": ["proxy_endpoint"],
    "analytic": ["related_analytics"],
}
Schema = dict[str, dict]
TypeMap = dict[str, str]


def log(msg: str):
    print("▶", msg, file=sys.stderr)


def load_schema() -> Schema:
    log(f"Fetching schema from {URL}")
    return requests.get(URL).json()


def patch_types(schema: Schema) -> None:
    log("Patching types")
    types = {}
    for type_name, type_def in schema["types"].items():
        if type_name in BASIC_TYPES:
            result = BASIC_TYPES[type_name]
        else:
            result = type_def["type"]
            if result in BASIC_TYPES:
                result = BASIC_TYPES[result]
        types[type_name] = result
    schema["types"] = types


class Writer:
    def __init__(self, file: typing.TextIO):
        self.file = file
        self.indent = 0

    def print(self, *args, **kwargs) -> None:
        print(self.indent * " ", file=self.file, end="")
        print(*args, **kwargs, file=self.file)

    def comment(self, text: str) -> None:
        width = COLUMN_LIMIT - self.indent - len("// ")
        lines = textwrap.wrap(text, width)
        for line in lines:
            self.print("//", line)

    def begin(self, *args, **kwargs) -> None:
        self.print(*args, **kwargs)
        self.indent += 2

    def end(self, *args, **kwargs) -> None:
        self.indent -= 2
        if self.indent < 0:
            raise ValueError
        self.print(*args, **kwargs)


def _emit(writer: Writer, schema: Schema, *, objects: bool) -> None:
    name = "objects" if objects else "classes"
    prefix = OBJECT_PREFIX if objects else CLASS_PREFIX
    types = schema["types"]
    first = True
    for entity in schema[name].values():
        if not first:
            writer.print()
        first = False
        if DOCUMENT_ENTITIES:
            writer.comment(entity["description"])
        entity_name = entity["name"]
        omit = OMIT.get(entity_name, [])
        writer.begin(f"type {prefix}{entity_name} = record{{")
        for attr_name, attr_def in sorted(entity["attributes"].items()):
            if attr_name in omit:
                continue
            profile = attr_def.get("profile")
            if profile is not None and PROFILES != ALL:
                if profile not in PROFILES:
                    continue
            if "object_type" in attr_def:
                type_name = attr_def["object_type"]
                # Some object names have a prefix, which is separated by a
                # slash. It looks like this prefix can just be discarded.
                slash = type_name.rfind("/")
                if slash != -1:
                    type_name = type_name[slash + 1 :]
                resolved = f"{OBJECT_PREFIX}{type_name}"
            else:
                resolved = types[attr_def["type"]]
                if resolved is OMIT_MARKER:
                    continue
            if attr_def.get("is_array", False):
                resolved = f"list<{resolved}>"
            if DOCUMENT_FIELDS:
                writer.comment(attr_def["description"])
            writer.print(f"{attr_name}: {resolved},")
        writer.end("}")


def emit_classes(writer: Writer, schema: Schema) -> None:
    log("Emitting class definitions")
    _emit(writer, schema, objects=False)


def emit_objects(writer: Writer, schema: Schema) -> None:
    log("Emitting object definitions")
    _emit(writer, schema, objects=True)


def open_schema_file():
    scripts = Path(__file__).parent
    types = (scripts / "../schema/types").resolve()
    if not types.is_dir():
        raise NotADirectoryError(f"expected {types} to be a directory")
    path = types / "ocsf.schema"
    log(f"Opening schema file {path}")
    return path.open("w")


def main():
    schema = load_schema()
    patch_types(schema)
    with open_schema_file() as f:
        writer = Writer(f)
        writer.comment("This file is generated, do not edit manually.")
        writer.comment(f"OCSF Version: {schema['version']}")
        profiles = (
            "all"
            if PROFILES == ALL
            else "none"
            if PROFILES == []
            else ", ".join(PROFILES)
        )
        writer.comment(f"OCSF Profiles: {profiles}")
        writer.print()
        emit_objects(writer, schema)
        writer.print()
        emit_classes(writer, schema)
    log("Done")


if __name__ == "__main__":
    main()
