VAST IPython Demo
=================

This demo provides two Jupyter notebooks to showcase how VAST can be used for
data analytics, using the python bridge `pyvast`. Follow the instructions below
to setup a VAST instance with demo data and install dependencies.


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

### Verify the Imported Data

```sh
# This is a sample of the original data from Suricata
vast export -n 10 json 'src_ip == 192.168.1.103'
# Pivot to Zeek conn logs
vast pivot zeek.conn 'src_ip == 192.168.1.103' | head -n 20
# Pivot to PCAP
vast pivot pcap.packet 'src_ip == 192.168.1.103' | tcpdump -nl -r -
```

## Jupyter Notebook

First install the required dependencies for your OS / environment. Then start
the Jupyter notebook for local interaction as follows.

### Setup

- With `NIX`
    ```sh
    nix-shell -I nixpkgs="https://github.com/NixOS/nixpkgs-channels/archive/cc6cf0a96a627e678ffc996a8f9d1416200d6c81.tar.gz" \
    -p "python3.withPackages(ps: [ps.notebook ps.numpy ps.matplotlib ps.pandas ps.pyarrow ps.networkx])"
    ```
- With Homebrew
    ```sh
    brew install jupyter numpy matplotlib pandas apache-arrow networkx
    ```
- Via Virtual Env
    ```sh
    virtualenv --system-site-packages venv
    source venv/bin/activate
    python3 -m pip install -r requirements.txt
    ```

Next, install `pyvast` with the following command. That can also be done in a
virtual env.

```sh
cd pyvast
python3 setup.py install
cd ..
```

### Startup

While in the current directory of this demo, start the notebook on localhost via

```sh
jupyter notebook
```

A browser tab should open automatically with a listing of the current directory.
You should now be able to click `vast-connections.ipynb` to open the prepared
notebook.
