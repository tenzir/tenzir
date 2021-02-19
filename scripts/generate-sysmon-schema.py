#! /usr/bin/env python

"""
Creates a sysmon schema for VAST.

Requirements:
    - pyyaml
    - hunters-forge/OSSEM as OSSEM in the CWD

Usage:
    - ./scripts/generate-sysmon-schema.py >schema/sysmon.schema
"""

import yaml
import textwrap
import os
import re


EVENTS_PATH = "OSSEM/source/data_dictionaries/windows/sysmon/events"


def format_title(data):
    """Formats the event name."""
    code = data["event_code"]
    if code == "1":
        return "ProcessCreation"
    elif code == "2":
        return "ProcessChangedFileCreationTime"
    elif code == "3":
        return "NetworkConnection"
    elif code == "4":
        return "SysmonServiceStateChanged"
    elif code == "5":
        return "ProcessTerminated"
    elif code == "6":
        return "DriverLoaded"
    elif code == "7":
        return "ImageLoaded"
    elif code == "8":
        return "CreateRemoteThread"
    elif code == "9":
        return "RawAccessRead"
    elif code == "10":
        return "ProcessAccess"
    elif code == "11":
        return "FileCreate"
    elif code == "12":
        return "RegistryEventObjectCreateAndDelete"
    elif code == "13":
        return "RegistryEventValueSet"
    elif code == "14":
        return "RegistryEventKeyAndValueRename"
    elif code == "15":
        return "FileCreateStreamHash"
    elif code == "16":
        return "SysmonConfigStateChanged"
    elif code == "17":
        return "PipeCreated"
    elif code == "18":
        return "PipeConnected"
    elif code == "19":
        return "WmiEventFilter"
    elif code == "20":
        return "WmiEventConsumer"
    elif code == "21":
        return "WmiEventConsumerToFilter"
    elif code == "22":
        return "DNSQuery"
    elif code == "23":
        return "FileDelete"
    elif code == "255":
        return "ErrorReport"
    else:
        raise NotImplementedError(data["title"])


def format_description(data):
    """Formats the event description."""
    return textwrap.indent(
        text=textwrap.fill(text=data["description"], width=77), prefix="// "
    )


def map_type(name, field_name):
    """Maps a Sysmon event type to VAST type."""
    if name == "string":
        return "string"
    elif name == "integer":
        if field_name == "SourcePort" or field_name == "DestinationPort":
            return "port"
        return "count"
    elif name == "date":
        return "timestamp"
    elif name == "boolean" or name == "bool":
        return "bool"
    elif name == "ip":
        return "addr"
    else:
        raise NotImplementedError(name)


def format_event_fields(data):
    """Generates a formatted record field entry for an event type."""
    for event_field in data["event_fields"]:
        yield textwrap.indent(
            text=f"""\
// {event_field['description']}
{event_field['name']}: {map_type(event_field['type'], event_field['name'])},""",
            prefix="    ",
        )


def format_event_type(data):
    """Formats the record type."""
    nl = "\n"
    return f"""\
{format_description(data)}
type sysmon.{format_title(data)} = record {{
{nl.join([event_field for event_field in format_event_fields(data)])}
}}
"""


def generate_event_types():
    """Generates the record types for all events in EVENTS_PATH."""
    for filename in sorted(os.listdir(EVENTS_PATH)):
        if filename.endswith(".yml"):
            with open(f"{EVENTS_PATH}/{filename}") as f:
                data = yaml.safe_load(f)
                if data["event_code"] == "255":
                    # TODO: ErrorReport is an empty event, which is not
                    # something that VAST currently supports.
                    continue
                yield format_event_type(data)


def main():
    for event_type in generate_event_types():
        print(event_type)


if __name__ == "__main__":
    main()
