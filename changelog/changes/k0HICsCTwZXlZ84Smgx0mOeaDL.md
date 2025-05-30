---
title: "Fix RFC3164 (legacy-syslog) parser expecting spaces after `<PRI>`"
type: bugfix
authors: eliaskosunen
pr: 3718
---

The RFC 3164 syslog parser no longer requires a whitespace after the `PRI`-field
(part in angle brackets in the beginning of a message).
