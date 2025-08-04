---
title: "Check if your data is truly empty"
type: feature
authors: mavam
pr: 5403
---

Ever stared at your security logs wondering if that suspicious-looking field is
actually empty or just pretending? We've all been there. That's why we added
`is_empty()` - a universal emptiness detector that works on strings, lists, and
records.

```tql
from {
  id: "evt-12345",
  tags: ["malware", "c2"],
  metadata: {},
  description: "",
  iocs: []
}
has_metadata = not metadata.is_empty()
has_description = not description.is_empty()
has_iocs = not iocs.is_empty()
```

```tql
{
  id: "evt-12345",
  tags: [
    "malware",
    "c2",
  ],
  metadata: {},
  description: "",
  iocs: [],
  has_metadata: false,
  has_description: false,
  has_iocs: false,
}
```

No more checking `length() == 0` or wondering if that field exists but is
empty. Just ask `is_empty()` and move on with your threat hunting!
