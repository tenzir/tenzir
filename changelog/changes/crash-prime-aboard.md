---
title: "New string padding functions"
type: feature
authors: mavam
pr: 5344
---

Ever tried aligning threat actor names in your incident reports? Or formatting
CVE IDs with consistent spacing for your vulnerability dashboard? We've all been
there, fighting with inconsistent string lengths that make our security tools
output look like alphabet soup. 🍲

Meet your new formatting friends: `pad_left()` and `pad_right()`!

#### Live Threat Feed Dashboard

Create a real-time threat indicator board with perfectly aligned columns:

```tql
from {time: "14:32", actor: "APT29", target: "energy", severity: 9},
     {time: "14:35", actor: "Lazarus", target: "finance", severity: 10},
     {time: "14:41", actor: "APT1", target: "defense", severity: 8}
select threat_line = time + " │ " + actor.pad_right(12) + " │ " +
                     target.pad_right(10) + " │ " + severity.string().pad_left(2, "0")
write_lines
```

```
14:32 │ APT29        │ energy     │ 09
14:35 │ Lazarus      │ finance    │ 10
14:41 │ APT1         │ defense    │ 08
```

#### CVE Priority Matrix

Format CVE IDs and CVSS scores for your vulnerability management system:

```tql
from {cve: "CVE-2024-1337", score: 9.8, vector: "network", status: "🔴"},
     {cve: "CVE-2024-42", score: 7.2, vector: "local", status: "🟡"},
     {cve: "CVE-2024-31415", score: 5.4, vector: "physical", status: "🟢"}
select priority = status + " " + cve.pad_right(16) + " [" +
                  score.string().pad_left(4) + "] " + vector.pad_left(10, "·")
write_lines
```

```
🔴 CVE-2024-1337    [ 9.8] ···network
🟡 CVE-2024-42      [ 7.2] ·····local
🟢 CVE-2024-31415   [ 5.4] ··physical
```

#### Network Flow Analysis

Build clean firewall logs with aligned source/destination pairs:

```tql
from {src: "10.0.0.5", dst: "8.8.8.8", proto: "DNS", bytes: 234},
     {src: "192.168.1.100", dst: "13.107.42.14", proto: "HTTPS", bytes: 8924},
     {src: "172.16.0.50", dst: "185.199.108.153", proto: "SSH", bytes: 45812}
select flow = src.pad_left(15) + " → " + dst.pad_left(15) +
              " [" + proto.pad_right(5) + "] " + bytes.string().pad_left(7) + " B"
write_lines
```

```
       10.0.0.5 →         8.8.8.8 [DNS  ]     234 B
  192.168.1.100 →    13.107.42.14 [HTTPS]    8924 B
    172.16.0.50 → 185.199.108.153 [SSH  ]   45812 B
```

Both padding functions accept three parameters:

- **String to pad** (required)
- **Target length** (required)
- **Padding character** (optional, defaults to space)

If your string is already longer than the target length, it returns unchanged.
Multi-character padding? That's a paddlin' (returns an error).

Your SOC dashboards never looked so clean! 🎯
