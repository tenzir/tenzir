The `syslog` parser no longer crops messages at unprintable characters, such as
tab (`\t`).

The `syslog` parser no longer eagerly attempts to grab an application name from
the content, fixing issues when combined with CEF and LEEF.
