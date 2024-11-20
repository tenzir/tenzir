# User Guides

The usage guides walk you through typical use cases that you perform..

## Datasets

Throughout the guides, we use publicly available datasets so that you can follow
along.

### M57

The [M57 Patents
Scenario](https://digitalcorpora.org/corpora/scenarios/m57-patents-scenario/)
contains large amounts of diverse network traffic. We enriched the PCAP from Nov
18, 2009, by adding malicious traffic from
[malware-traffic-analysis.net](https://malware-traffic-analysis.net). We
adjusted all packet timestamp to 2021. Thereafter, we ran
[Zeek](https://zeek.org) v5.2.0 and [Suricata](https://suricata.io) 6.0.10 to
obtain structured logs.

The dataset includes the following files:

- [README.md](https://storage.googleapis.com/tenzir-datasets/M57/README.md)
- [zeek-all.log.zst](https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst) (41 MB)
- [suricata.json.zst](https://storage.googleapis.com/tenzir-datasets/M57/suricata.json.zst) (57 MB)
- [data.pcap](https://storage.googleapis.com/tenzir-datasets/M57/PCAP/data.pcap) (3.8 GB)

For following examples we assume that you have imported the demo data in your
node with the following two pipelines:

```
from https://storage.googleapis.com/tenzir-datasets/M57/suricata.json.zst 
  read suricata --no-infer
| where #schema != "suricata.stats"
| import
```

```
from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst
  read zeek-tsv
| import
```

Note that the demo node already comes with this demo data pre-populated for you.

import DocCardList from '@theme/DocCardList';

<DocCardList />
