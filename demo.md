# Setup

### Starting the Node
```
rm -rf demo.db
build/bin/vast -d demo.db start
```

### Import demo data
```
build/bin/vast import zeek -r /Volumes/GoogleDrive/Shared\ drives/Engineering/Data/Zeek/M57-2009-day11-18/conn.log
build/bin/vast import suricata -r /Volumes/GoogleDrive/Shared\ drives/Engineering/Data/Suricata/M57-day11-18.json
build/bin/vast import pcap -r /Volumes/GoogleDrive/Shared\ drives/Engineering/Data/PCAP/M57-2009-day11-18.trace
```

# Pivot Demo

```
# This is a sample of the original data from suricata
build/bin/vast export -n 10 json 'src_ip == 192.168.1.103'
# Pivot to Zeek conn logs
build/bin/vast pivot zeek.conn 'src_ip == 192.168.1.103' | head -n 20
# Pivot to PCAP
build/bin/vast pivot pcap.packet 'src_ip == 192.168.1.103' | tcpdump -nl -r -
```

# Jupyter notebook with pandas

Setup with NIX:
```
nix-shell -I nixpkgs=$HOME/projects/nixpkgs -p "python3.withPackages(ps: [ps.notebook ps.numpy ps.matplotlib ps.pandas ps.pyarrow ps.networkx])"
```

Setup with brew:
```
#brew install jupyter numpy matplotlib pandas apache-arrow networkx
python3 -mpip install matplotlib numpy pyarrow pandas networkx
```

then:
```
jupyter notebook
```
-> Browser Tab opens -> Open vast-connections.ipynb
