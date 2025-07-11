---
title: "New string padding functions"
type: feature
authors: mavam
pr: 5344
---

Ever tried aligning IP addresses in your threat intel reports? Or formatting
Windows SIDs with consistent padding for your SIEM correlation rules? We've all
been there, fighting with inconsistent string lengths that make our security
dashboards look like a ransom note. 📊

Meet your new formatting friends: `pad_left()` and `pad_right()`!

#### Aligning IP Addresses for Clean Reports

Security teams love their IP address tables. Now you can make them beautiful:

```tql
from {ip: "10.1.2.3", severity: "high", count: 42},
     {ip: "192.168.1.254", severity: "medium", count: 7},
     {ip: "172.16.0.1", severity: "low", count: 139},
set formatted_ip = ip.pad_left(15)
set display = formatted_ip + " | " + severity.pad_right(8) + " | " + count.string().pad_left(5)
```

```tql
{
  ip: "10.1.2.3",
  severity: "high",
  count: 42,
  formatted_ip: "      10.1.2.3",
  display: "      10.1.2.3 | high     |    42"
}
{
  ip: "192.168.1.254",
  severity: "medium",
  count: 7,
  formatted_ip: " 192.168.1.254",
  display: " 192.168.1.254 | medium   |     7"
}
{
  ip: "172.16.0.1",
  severity: "low",
  count: 139,
  formatted_ip: "    172.16.0.1",
  display: "    172.16.0.1 | low      |   139"
}
```

#### Formatting Hashes and IDs

Make your threat indicators consistent across different tools:

```tql
from {type: "MD5", hash: "d41d8cd98f00b204e9800998ecf8427e"},
     {type: "SHA1", hash: "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
     {type: "SHA256", hash: "e3b0c44298fc1c149afbf4c8996fb924"}
set label = type.pad_right(8, ".") + ": " + hash.pad_right(64, "-")
```

```tql
{
  type: "MD5",
  hash: "d41d8cd98f00b204e9800998ecf8427e",
  label: "MD5.....: d41d8cd98f00b204e9800998ecf8427e--------------------------------"
}
{
  type: "SHA1",
  hash: "da39a3ee5e6b4b0d3255bfef95601890afd80709",
  label: "SHA1....: da39a3ee5e6b4b0d3255bfef95601890afd80709------------------------"
}
{
  type: "SHA256",
  hash: "e3b0c44298fc1c149afbf4c8996fb924",
  label: "SHA256..: e3b0c44298fc1c149afbf4c8996fb924--------------------------------"
}
```

In plain text:

```txt
MD5.....: d41d8cd98f00b204e9800998ecf8427e--------------------------------
SHA1....: da39a3ee5e6b4b0d3255bfef95601890afd80709------------------------
SHA256..: e3b0c44298fc1c149afbf4c8996fb924--------------------------------
```

#### Windows Security Identifiers (SIDs)

Format those RID components with leading zeros for consistent SIEM parsing:

```tql
from {user: "admin", rid: "500"},
     {user: "guest", rid: "501"},
     {user: "john.doe", rid: "1142"}
set sid = "S-1-5-21-3623811015-3361044348-30300820-" + rid.pad_left(4, "0")
set entry = user.pad_right(15) + " -> " + sid
```

```tql
{
  user: "admin",
  rid: "500",
  sid: "S-1-5-21-3623811015-3361044348-30300820-0500",
  entry: "admin           -> S-1-5-21-3623811015-3361044348-30300820-0500"
}
{
  user: "guest",
  rid: "501",
  sid: "S-1-5-21-3623811015-3361044348-30300820-0501",
  entry: "guest           -> S-1-5-21-3623811015-3361044348-30300820-0501"
}
{
  user: "john.doe",
  rid: "1142",
  sid: "S-1-5-21-3623811015-3361044348-30300820-1142",
  entry: "john.doe        -> S-1-5-21-3623811015-3361044348-30300820-1142"
}
```

In plain text:

```txt
admin           -> S-1-5-21-3623811015-3361044348-30300820-0500
guest           -> S-1-5-21-3623811015-3361044348-30300820-0501
john.doe        -> S-1-5-21-3623811015-3361044348-30300820-1142
```

#### Creating Visual Separators

Build clean log separators and headers for your incident reports:

```tql
from {title: "INCIDENT #2024-1337"}
set header = "=".pad_right(50, "=")
set centered = title.pad_left(30).pad_right(50)
set footer = "=".pad_right(50, "=")
```

```tql
{
  title: "INCIDENT #2024-1337",
  header: "==================================================",
  centered: "           INCIDENT #2024-1337                    ",
  footer: "=================================================="
}
```

#### Port Number Alignment

Keep your network logs tidy with aligned port numbers:

```tql
from {service: "ssh", port: 22},
    {service: "https", port: 443},
    {service: "rdp", port: 3389},
    {service: "vnc", port: 5900}
set display = service.pad_right(10) + " : " + port.string().pad_left(5, "0")
```

```tql
{service: "ssh", port: 22, display: "ssh        : 00022"}
{service: "https", port: 443, display: "https      : 00443"}
{service: "rdp", port: 3389, display: "rdp        : 03389"}
{service: "vnc", port: 5900, display: "vnc        : 05900"}
```

In plain text:

```txt
ssh        : 00022
https      : 00443
rdp        : 03389
vnc        : 05900
```

#### Unicode Support

Yes, it works with Unicode too! Perfect for those international incident
responses:

```tql
from {status: "🔴", message: "Critical"},
     {status: "🟡", message: "Warning"},
     {status: "🟢", message: "OK"}
set line = status + message.pad_left(12, "·")
```

```tql
{status: "🔴", message: "Critical", line: "🔴····Critical"}
{status: "🟡", message: "Warning", line: "🟡·····Warning"}
{status: "🟢", message: "OK", line: "🟢··········OK"}
```

In text:

```txt
🔴····Critical
🟡·····Warning
🟢··········OK
```

Both padding functions accept three parameters:

- **String to pad** (required)
- **Target length** (required)
- **Padding character** (optional, defaults to space)

If your string is already longer than the target length, it returns unchanged.
Multi-character padding? That's a paddlin' (returns null with a warning).

Security never looked so tidy! 🎯
