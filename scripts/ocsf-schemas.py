#!/usr/bin/env python3

from pathlib import Path
import sys
import textwrap
import typing
import re
from typing import Optional
import contextlib

import requests

ALL = object()

# ================== Configuration ================== #
SERVER = "https://schema.ocsf.io"
DOCUMENT_ENTITIES = True
DOCUMENT_FIELDS = False
OCSF_PREFIX = "_ocsf"
OBJECT_INFIX = "object"
COLUMN_LIMIT = 80
PROFILES = ALL  # List of strings or ALL.
EXCLUDE_VERSIONS = ["1.0.0-rc.2", "1.0.0-rc.3"]
ROOT_DIR = Path(__file__).parent.parent

# TODO: Discuss dropping optional fields.
# =================================================== #

OMIT_MARKER = object()
BASIC_TYPES = {
    "boolean_t": "bool",
    "float_t": "double",
    "integer_t": "int64",
    "json_t": "string #print_json",
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


log_indent = 0

def log(msg: str):
    print(" " * log_indent + "▶", msg, file=sys.stderr, flush=True)


@contextlib.contextmanager
def log_section(msg: str):
    global log_indent
    log(msg)
    log_indent += 2
    yield
    log_indent -= 2


def load_schema(version: Optional[str] = None) -> Schema:
    url = SERVER
    if version is not None:
        url += "/" + version
    url += "/export/schema"
    log(f"Fetching schema from {url}")
    return requests.get(url).json()


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


def class_prefix(schema: Schema) -> str:
    return OCSF_PREFIX + ".v" + schema["version"].replace(".", "_").replace("-", "_")


def object_prefix(schema: Schema) -> str:
    return class_prefix(schema) + "." + "object"


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
    prefix = object_prefix(schema) if objects else class_prefix(schema)
    types = schema["types"]
    first = True
    for entity in schema[name].values():
        if not first:
            writer.print()
        first = False
        if DOCUMENT_ENTITIES:
            writer.comment(entity["description"])
        # For classes, we do not use the given entity name, but instead derive
        # the name from the caption. This is due to classes such as "Device
        # Inventory Info", which have a name "inventory_info", which makes the
        # name hard to predict from the class name, or even misspelled ones.
        if objects:
            entity_name = entity["name"]
        else:
            entity_name = entity["caption"].lower().replace(" ", "_")
        if extension := entity.get("extension"):
            entity_name = extension + "__" + entity_name
        full_name = f"{prefix}.{entity_name}"
        if full_name == "ocsf.object.object":
            # Not needed because this is special-case to print JSON.
            continue
        omit = OMIT.get(entity_name, [])
        writer.begin(f"type {full_name} = record{{")
        for attr_name, attr_def in sorted(entity["attributes"].items()):
            if attr_name in omit:
                continue
            profile = attr_def.get("profile")
            if profile is not None and PROFILES != ALL:
                if profile not in PROFILES:
                    continue
            if "object_type" in attr_def:
                type_name = attr_def["object_type"]
                # Special-case the "Object" type to use a JSON string instead.
                if type_name == "object":
                    resolved = "string #print_json"
                else:
                    type_name = type_name.replace("/", "__")
                    # Some object names have a prefix, which is separated by a
                    # slash. It looks like this prefix can just be discarded.
                    # slash = type_name.rfind("/")
                    # if slash != -1:
                    #     log(f"{full_name} -> {type_name}")
                    #     type_name = type_name[slash + 1 :]
                    resolved = f"{object_prefix(schema)}.{type_name}"
            else:
                resolved = types[attr_def["type"]]
                if resolved is OMIT_MARKER:
                    continue
            if attr_def.get("is_array", False):
                resolved = f"list<{resolved}>"
            if DOCUMENT_FIELDS:
                writer.comment(attr_def["description"])
            requirement = attr_def["requirement"]
            attributes = f" #{requirement}"
            if profile is not None:
                attributes += f" #profile={profile}"
            extension = attr_def.get("extension")
            if extension is not None:
                attributes += f" #extension={extension}"
            writer.print(f"{attr_name}: {resolved}{attributes},")
        writer.end("}")


def emit_classes(writer: Writer, schema: Schema) -> None:
    _emit(writer, schema, objects=False)


def emit_objects(writer: Writer, schema: Schema) -> None:
    _emit(writer, schema, objects=True)


def open_schema_file(version: str):
    version = re.sub("[^0-9a-zA-Z_-]", "", version.replace(".", "_"))
    name = "v" + version + ".schema"
    types = ROOT_DIR / "schema/types"
    if not types.is_dir():
        raise NotADirectoryError(f"expected {types} to be a directory")
    ocsf_dir = types / "ocsf"
    ocsf_dir.mkdir(exist_ok=True)
    path = ocsf_dir / name
    log(f"Writing schema file {path}")
    return path.open("w")


def collect_enum(schema: Schema, attribute: str) -> dict[int, str]:
    result = {}
    for entity in schema["classes"].values():
        enum = entity["attributes"][attribute]["enum"]
        for key, value in enum.items():
            num = int(key)
            name = value["caption"]
            if num in result:
                if result[num] != name:
                    raise ValueError("got mismatch")
            result[num] = name
    return dict(sorted(result.items()))


def write_enum(schema: Schema, attribute: str, filename: str) -> None:
    categories = collect_enum(schema, attribute)
    categories_inc = ROOT_DIR / "libtenzir/include/tenzir" / filename
    with categories_inc.open("w") as f:
        f.write("// This file was generated, do not edit.\n\n")
        for num, name in sorted(categories.items()):
            f.write(f'X({num}, "{name}")\n')


def write_enums(schema: Schema):
    write_enum(schema, "category_uid", "ocsf_categories.inc")
    write_enum(schema, "class_uid", "ocsf_classes.inc")
    write_enum(schema, "type_uid", "ocsf_types.inc")


def fetch_versions() -> list[str]:
    # return ["1.5.0"]
    log(f"Fetching available versions from {SERVER}")
    body = requests.get(SERVER).content.decode()
    return [
        version
        for version in re.findall("<option value=[^>]*>v([^<]*)</option>", body)
        if version not in EXCLUDE_VERSIONS
    ]


def main():
    versions = fetch_versions()
    for version in versions:
        with log_section(f"Processing version {version}"):
            schema = load_schema(version)
            patch_types(schema)
            with open_schema_file(version) as f:
                writer = Writer(f)
                writer.comment("This file is generated, do not edit manually.")
                writer.comment(f"OCSF version: {version}")
                # profiles = (
                #     "all"
                #     if PROFILES == ALL
                #     else "none" if PROFILES == [] else ", ".join(PROFILES)
                # )
                # writer.comment(f"OCSF Profiles: {profiles}")
                writer.print()
                emit_objects(writer, schema)
                writer.print()
                emit_classes(writer, schema)
            write_enums(schema)
    log("Done")


if __name__ == "__main__":
    main()
