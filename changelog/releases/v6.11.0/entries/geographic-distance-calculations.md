---
title: Geographic distance calculations
type: feature
authors:
  - mavam
created: 2026-08-13T13:22:13.4632Z
---

The new `geo_distance` function calculates the surface distance in meters between two longitude/latitude pairs. It uses a fast spherical calculation by default and can account for the WGS-84 spheroid when you need greater accuracy:

```tql
from {
  distance_m: geo_distance(13.405, 52.52, -0.1276, 51.5072),
  precise_distance_m: geo_distance(
    13.405,
    52.52,
    -0.1276,
    51.5072,
    spheroid=true,
  ),
}
```

```tql
{
  distance_m: 931561.8960448167,
  precise_distance_m: 934514.4909447534,
}
```

Invalid, non-finite, and null coordinates produce `null`.
