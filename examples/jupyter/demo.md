VAST IPython Demo
=================

This demo provides two Jupyter notebooks to showcase how VAST can be used for
data analytics, using the python bridge `pyvast`. Follow the instructions below
to setup a VAST instance with demo data and install dependencies.

## Prepare the environment

This demo assumes that `vast` is available in `PATH` in all involved shells,
specifically also the one used to start the Jupyter Notebook later. Add it
with the following command:

##### Bash/Zsh
```sh
export PATH=$(git rev-parse --show-toplevel)/build/bin:$PATH
```
##### Fish
```sh
set -gx PATH (git rev-parse --show-toplevel)/build/bin $PATH
```

## Start a VAST Node

```sh
rm -rf demo.db
vast -d demo.db start
```

### Download Demo Data

The demo data, packaged as `tar`-ball, is publicly available
[here](https://storage.googleapis.com/tenzir-public-data/vast-jupyter-demo.tgz).

Follow these steps to download and unpack the data.

```sh
curl -L -o vast-jupyter-demo.tgz https://storage.googleapis.com/tenzir-public-data/vast-jupyter-demo.tgz
tar xzf vast-jupyter-demo.tgz
```

Once unpacked, the archive provides a folder with three files:

- vast-jupyter-demo/M57-2009-day11-18.trace
- vast-jupyter-demo/M57-day11-18-conn.log
- vast-jupyter-demo/M57-day11-18.json

### Import Demo Data

```sh
vast import zeek -r vast-jupyter-demo/M57-2009-day11-18/conn.log
vast import suricata -r vast-jupyter-demo/M57-day11-18.json
vast import pcap -r vast-jupyter-demo/M57-2009-day11-18.trace
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
- With `venv`
    ```sh
    python3 -m venv venv
    source venv/bin/activate
    # Verify that the python from venv is used:
    which python
    python -m pip install wheel
    python -m pip install -r requirements.txt
    ipython kernel install --user --name=venv
    ```

As long as the demo is started from this directory, the step of installing
`pyvast` can be skipped, because the working tree copy is available locally.
If you want to install `pyvast` nonethelesss, run the following command:

```sh
cd "$(git rev-parse --show-toplevel)/pyvast"
python setup.py install
cd -
```

### Startup

While in the current directory of this demo (and inside of the `venv`, if that
is used for the setup), start the notebook on localhost via

```sh
jupyter notebook
```

A browser tab should open automatically with a listing of the current directory.
You should now be able to click `vast-connections.ipynb` to open the prepared
notebook.
