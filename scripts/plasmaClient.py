#! /usr/bin/env python3
import pyarrow as pa
import pyarrow.plasma as plasma

import binascii
import argparse

def main():
    parser = argparse.ArgumentParser(
        description='Plasma Client'
    )
    parser.add_argument(
        '--socket', '-s', required=False, help='default /tmp/plasma'
    )
    parser.add_argument(
        '--id', '-i', required=True, help='Plasma Object Id'
    )
    args = parser.parse_args()

    socket = "/tmp/plasma"
    if args.socket is not None:
        socket = args.socket

    client = plasma.connect(socket, "", 0)

    [buffers] = client.get_buffers(
        [plasma.ObjectID(binascii.unhexlify(args.id))]
    )
    
    data = pa.BufferReader(buffers)
    
    batch = pa.RecordBatchStreamReader(data)
    all = batch.read_all()
    
    for b in all:
        print(b.name, b.data.to_pylist())

if __name__ == '__main__':
    main()
