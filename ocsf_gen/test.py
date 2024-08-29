#!/usr/bin/env python3

import json
from pprint import pprint
import textwrap
import sys

BASIC_TYPES = {
    "boolean_t": "bool",
    "float_t": "double",
    "integer_t": "int64",
    # TODO: Dont' delete this.
    "json_t": None,
    "long_t": "int64",
    "string_t": "string",
    "bytestring_t": "blob",
    "datetime_t": "time",
    "ip_t": "ip",
    "subnet_t": "subnet",
    # TODO: Is this the right choice?
    "timestamp_t": "time",
}
# TODO: Maybe some recursive unfolding up to a certain depth?
# parent object name -> field object name
OMIT = {
    "ldap_person": "user",
    "process": "process",
    "network_proxy": "network_proxy",
    "analytic": "analytic",
}

schema = json.load(open("schema.json"))
types = {}
for type_name, type_def in schema["types"].items():
    if type_name in BASIC_TYPES:
        result = BASIC_TYPES[type_name]
    else:
        result = type_def["type"]
        # TODO: Might have to do this multiple times.
        if result in BASIC_TYPES:
            result = BASIC_TYPES[result]
    types[type_name] = result


def print_sth(name, prefix=""):
    if prefix != "":
        prefix += "."
    for entry in schema[name].values():
        for line in textwrap.wrap(entry["description"], 77):
            print("//", line)
        object_name = entry["name"]
        print(f"type ocsf.{prefix}{object_name} = record{{")
        for attr_name, attr_def in entry["attributes"].items():
            # for line in textwrap.wrap(attr_def["description"], 75):
            #     print("  //", line)
            if "profile" in attr_def:
                continue
            if "object_type" in attr_def:
                type_name = attr_def["object_type"]
                # TODO: Stripping the / might not be the best way.
                slash = type_name.rfind("/")
                if slash != -1:
                    type_name = type_name[slash + 1 :]
                if OMIT.get(object_name) == type_name:
                    continue
                print(f"  {attr_name}: ", end="")
                print(f"ocsf.objects.{type_name}", end="")
            else:
                resolved = types[attr_def["type"]]
                if resolved is None:
                    continue
                print(f"  {attr_name}: ", end="")
                print(resolved, end="")
            print(",")
        print("}\n")


print_sth("objects", "objects")
print_sth("classes")
