Ingest Syslog messages into VAST. The following formats are supported:
- [RFC 5424](https://tools.ietf.org/html/rfc5424)
- A fallback format that consists only of the Syslog message.

```bash
# Import from file.
vast import syslog --read=path/to/sys.log

# Continuously import from a stream.
syslog | vast import syslog
```
