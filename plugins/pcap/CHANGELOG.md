# Changelog

This changelog documents all notable changes to the PCAP plugin for VAST.

## v1.1.0

### Features

- VAST no longer discards parsed 802.1Q VLAN tags but instead writes them into
  a new record `vlan`. This record contains two fields `outer` and `inner` that
  correspond to the outer and inner VLAN identifier (VID), respectively.
  [#2179](https://github.com/tenzir/vast/pull/2179)

### Changes

- The option `--disable-community-id` no longer removes the field
  `community_id` from the schema. Rather, it simply sets the field values to
  null.
  [#2179](https://github.com/tenzir/vast/pull/2179)

## v1.0.0

This is the first official release.
