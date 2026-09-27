#!/bin/sh
# runner: shell
# timeout: 180
set -eu

input_dir=$(mktemp -d)
for workload in homogeneous nullheavy conflict; do
  awk -v workload="$workload" 'BEGIN {
    for (i=0; i<80000; ++i) {
      block=int(i/10000)%2;
      x=(workload=="nullheavy" ? (block ? "\"value\"" : "null") :
         workload=="conflict" ? (block ? "\"value\"" : "42") : "42");
      y=(workload=="nullheavy" && block ? "null" : "7");
      printf "{\"id\":%d,\"x\":%s,\"y\":%s,\"message\":\"security telemetry event\",\"tags\":[\"network\",\"sample\"],\"nested\":{\"ok\":true,\"code\":200}}\n",i,x,y;
    }
  }' >"$input_dir/$workload.ndjson"
  "$TENZIR_BINARY" --nova --bare-mode --console-verbosity=error \
    --endpoint="$TENZIR_NODE_CLIENT_ENDPOINT" "
    from_file \"$input_dir/$workload.ndjson\" { read_json }
    @name = \"buffered.$workload\"
    import
  "
  "$TENZIR_BINARY" --bare-mode --console-verbosity=error \
    --endpoint="$TENZIR_NODE_CLIENT_ENDPOINT" "
    remote {
      partitions
      where schema == \"buffered.$workload\"
      summarize partitions=count(), events=sum(events)
      write_ndjson
    }
  "
  "$TENZIR_BINARY" --bare-mode --console-verbosity=error \
    --endpoint="$TENZIR_NODE_CLIENT_ENDPOINT" "
    export
    where @name == \"buffered.$workload\"
    summarize count=count()
    write_ndjson
  "
done
