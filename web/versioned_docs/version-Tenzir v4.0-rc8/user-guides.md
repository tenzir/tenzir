# User Guides

The user guides walk you through various examples that illustrate how to use use
Tenzir in practice.

## Datasets

Throughout our guides, we use publicly available datasets for a reproducible
experience.

### M57

The [M57 Patents
Scenario](https://digitalcorpora.org/corpora/scenarios/m57-patents-scenario/)
contains large amounts of diverse network traffic. We enriched the PCAP from Nov
18, 2009, by adding malicious traffic from
[malware-traffic-analysis.net](https://malware-traffic-analysis.net). We
adjusted all packet timestamp to 2021. Thereafter, we ran
[Zeek](https://zeek.org) v5.2.0 and [Suricata](https://suricata.io) 6.0.10 to
obtain structured network logs.

The dataset includes the following files:

- [README.md](https://storage.googleapis.com/tenzir-datasets/M57/README.md)
- [zeek.tar.gz](https://storage.googleapis.com/tenzir-datasets/M57/zeek.tar.gz) (43 MB)
- [suricata.tar.gz](https://storage.googleapis.com/tenzir-datasets/M57/suricata.tar.gz) (123 MB)
- [data.pcap](https://storage.googleapis.com/tenzir-datasets/M57/PCAP/data.pcap) (3.8 GB)

For the examples in the next section, download and extract the archives:

```bash
curl -L -O https://storage.googleapis.com/tenzir-datasets/M57/suricata.tar.gz
curl -L -O https://storage.googleapis.com/tenzir-datasets/M57/zeek.tar.gz
tar xzvf suricata.tar.gz
tar xzvf zeek.tar.gz
```

import DocCardList from '@theme/DocCardList';

<DocCardList />
