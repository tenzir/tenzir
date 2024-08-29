#!/usr/bin/env python3

import json
from pprint import pprint
import textwrap
import sys

basic_types = {
    "boolean_t": "bool",
    "float_t": "double",
    "integer_t": "int64",
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

schema = json.load(open("schema.json"))
types = {}
for type_name, type_def in schema["types"].items():
    if type_name in basic_types:
        result = basic_types[type_name]
    else:
        result = type_def["type"]
        # TODO: Might have to do this multiple times.
        if result in basic_types:
            result = basic_types[result]
    types[type_name] = result

c = 0
for object in schema["objects"].values():
    # for line in textwrap.wrap(object["description"], 77):
    #     print("//", line)
    print(f"type ocsf.objects.{object["name"]} = record{{")
    for attr_name, attr_def in object["attributes"].items():
        # for line in textwrap.wrap(attr_def["description"], 75):
        #     print("  //", line)
        print(f"  {attr_name}: ", end="")
        if "object_type" in attr_def:
            name = attr_def["object_type"]
            # TODO: Stripping the / might not be the best way.
            slash = name.rfind("/")
            if slash != -1:
                name = name[slash+1:]
            print(f"ocsf.objects.{name}", end="")
        else:
            print(types[attr_def["type"]], end="")
        print(",")
    print("}\n")
    if c > 999:
        break
    c += 1


# schema["classes"] = {"authentication": schema["classes"]["authentication"]}

# for class_ in schema["classes"].values():
#     print(f"type ocsf.{class_["name"]} = record{{")
#     print("}\n")
