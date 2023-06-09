#! /usr/bin/env python3

# Example usage:
# vast -N export arrow '#type == /suricata.*/' | ./scripts/print-arrow-batch-size.py

import sys
import pyarrow

# Open stdin in binary mode.
istream = pyarrow.input_stream(sys.stdin.buffer)

# An Arrow reader consumes a stream of batches with the same schema. When
# reading the result for a query that returns multiple schemas, VAST will use
# multiple writers. Hence, we try to open record batch readers until an
# exception occurs.
try:
    while True:
        reader = pyarrow.ipc.RecordBatchStreamReader(istream)
        try:
            while True:
                batch = reader.read_next_batch()
                print(
                    f"{batch.schema.metadata[b'TENZIR:name:0'].decode('utf-8')}: {batch.num_rows}"
                )
        except StopIteration:
            pass
except:
    pass
