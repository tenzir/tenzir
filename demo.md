# Setup

## Start a VAST Node

```sh
rm -rf demo.db
vast -d demo.db start
```

### Import Demo Data

```sh
ln -s /path/to/data
vast import zeek -r data/Zeek/M57-2009-day11-18/conn.log
vast import suricata -r data/Suricata/M57-day11-18.json
vast import pcap -r data/PCAP/M57-2009-day11-18.trace
```

# Pivot to Zeek Logs and PCAP Flows

```sh
# This is a sample of the original data from Suricata
vast export -n 10 json 'src_ip == 192.168.1.103'
# Pivot to Zeek conn logs
vast pivot zeek.conn 'src_ip == 192.168.1.103' | head -n 20
# Pivot to PCAP
vast pivot pcap.packet 'src_ip == 192.168.1.103' | tcpdump -nl -r -
```

# Jupyter Notebook with pandas

Setup with NIX:
```sh
nix-shell -I nixpkgs="https://github.com/NixOS/nixpkgs-channels/archive/cc6cf0a96a627e678ffc996a8f9d1416200d6c81.tar.gz" \
 -p "python3.withPackages(ps: [ps.notebook ps.numpy ps.matplotlib ps.pandas ps.pyarrow ps.networkx])"
```

Setup with Homebrew:

```sh
#brew install jupyter numpy matplotlib pandas apache-arrow networkx
python3 -mpip install matplotlib numpy pyarrow pandas networkx
```

Then:

```
jupyter notebook
```

-> Browser Tab opens -> Open vast-connections.ipynb
