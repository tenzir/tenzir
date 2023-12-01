The record created by the RFC 3164 syslog parser no longer has a `tag` field,
but `app_name` and `process_id`. Additionally, a `message` field is added,
which contains the unparsed MESSAGE-part of the syslog message.
