---
title: XML parsing functions for TQL
type: feature
authors:
  - mavam
  - claude
created: 2025-12-30T17:54:38.825139Z
---

The new `parse_xml` and `parse_winlog` functions parse XML strings into structured records, enabling analysis of XML-formatted logs and data sources.

The `parse_xml` function offers flexible XML parsing with XPath-based element selection, configurable attribute handling, namespace management, and depth limiting. It supports multiple match results as lists and handles both simple and complex XML structures.

The `parse_winlog` function specializes in parsing Windows Event Log XML format, automatically finding Event elements and transforming EventData/UserData sections into properly structured fields.

Both functions integrate with Tenzir's multi-series builder for schema inference and type handling.
