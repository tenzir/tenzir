#! /usr/bin/env python3

# Example usage:
# vast -N export arrow '#type == /suricata.*/' | ./scripts/print-arrow.py

import sys
import pyarrow

# Open stdin in binary mode.
istream = pyarrow.input_stream(sys.stdin.buffer)
batch_count = 0
row_count = 0

# An Arrow reader consumes a stream of batches with the same schema. When
# reading the result for a query that returns multiple schemas, VAST will use
# multiple writers. Hence, we try to open record batch readers until an
# exception occurs.
try:
    while True:
        print("open next reader")
        reader = pyarrow.ipc.RecordBatchStreamReader(istream)
        try:
            while True:
                batch = reader.read_next_batch()
                batch_count += 1
                row_count += batch.num_rows
                print(str(batch.schema))
        except StopIteration:
            print("done with current reader, rows: " + str(row_count))
            batch_count = 0
            row_count = 0
except:
    print("done with all readers")
