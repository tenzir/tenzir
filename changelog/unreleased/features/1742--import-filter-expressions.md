VAST now supports import filter expressions. They function as the dual to export
query expressions: `vast import suricata '#type == "suricata.alert"' <
path/to/eve.json` will import only `suricata.alert` events, discarding all other
events.
