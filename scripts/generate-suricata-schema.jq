#! /usr/bin/env jq -rMf

# Usage:
#   ./scripts/generate-suricata-schema.jq path/to/eve.json.schema
#
# Notes:
#   See https://github.com/satta/suricata-json-schema for how to obtain
#   a current eve.json.schema file.
#   This script is very barebones currently, and not intended to be used in
#   production. Its output requires further editing by a human.

def named_record(indent):
	.properties as $properties
	| $properties // empty | keys_unsorted | map(. as $field
		| $properties[$field].type as $type
		| if $type == "boolean" then 
			indent + ($field) + ": bool" 
		elif $type == "string" then 
			indent + ($field) + ": string" 
		elif $type == "array" then 
			indent + ($field) + ": vector<" + ($properties[$field].items | named_record(indent + "  ")) + ">" 
		elif $type == "object" then 
			indent + ($field) +  ": record{\n" + ($properties[$field] | named_record(indent +"  ")) + indent + "}"
		elif $type == "integer" then 
			indent + ($field) + ": count" 
		else
			""
		end + ",\n")
	| add;

. | named_record("")
