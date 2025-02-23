#!/usr/bin/env fish

echo $argv | read VAST DATA_ROOT

set STORE_TYPES feather parquet
set RUNS 8
# set OUT from_scratch.csv
set OUT clean_slate.csv


echo using VAST: $VAST
echo using DATA: $DATA_ROOT
echo output to: $OUT

echo "store_type,duration,bytes_memory,bytes_in_storage,num_events,num_slices,schema,zstd level" > $OUT
for store in {$STORE_TYPES}
  set DB vast.db_$store 
  set VAST_CMD $VAST -N --store-backend=$store -d $DB 
  echo executing benchmarks for store type \"{$store}\"
  rm -r $DB
  hyperfine --show-output --runs $RUNS \
    "cat $DATA_ROOT/Zeek/*.log | $VAST_CMD import zeek" 2>&1 | rg --color=auto "tp;" | cut -f2 -d\; | tee -a $OUT
  rm -r $DB
  hyperfine --show-output --runs $RUNS \
    "cat $DATA_ROOT/Suricata/eve.json | $VAST_CMD import suricata" 2>&1 | rg --color=auto "tp;" | cut -f2 -d\; | tee -a $OUT
    # "cat $DATA_ROOT/Suricata/eve.json | rg \"event_type\\\":\\\"flow\" | $VAST_CMD import suricata" 2>&1 | rg --color=auto "tp;" | cut -f2 -d\; | tee -a $OUT
  rm -r $DB
  hyperfine --show-output --runs $RUNS \
    "cat $DATA_ROOT/PCAP/data.pcap | $VAST_CMD import pcap" 2>&1 | rg --color=auto "tp;" | cut -f2 -d\; | tee -a $OUT
    # "cat $DATA_ROOT/PCAP/data.pcap | $VAST_CMD import pcap"
end

