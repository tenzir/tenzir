#!/bin/sh
# runner: shell
set -eu

# TENZIR_BINARY holds a command line, such as `tenzir run`.
tenzir() {
  # shellcheck disable=SC2086
  $TENZIR_BINARY "$@"
}

# REDIS_CLI holds a command line, such as `redis-cli -p 6379`.
cli() {
  # shellcheck disable=SC2086
  $REDIS_CLI "$@"
}

# Waits until `cli <args>` prints the expected output.
wait_for() {
  expected=$1
  shift
  attempts=0
  until [ "$(cli "$@")" = "$expected" ]; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 300 ]; then
      echo "timed out waiting for \`$*\` to print \`$expected\`" >&2
      exit 1
    fi
    sleep 0.1
  done
}

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
# A D4 worker for type 1 writes compressed pcap files and pushes their names
# to the analyzer queue in database 2.
queue="analyzer:1:5d6e2b4c-0f6a-4b8e-9d3c-7a1b2c3d4e5f"
for name in concatenated-vlan-files big-endian-single-packet; do
  file="$dir/$name.cap"
  cp "$TENZIR_INPUTS/pcap/$name.pcap" "$file"
  gzip "$file"
  cli -n 2 lpush "$queue" "$file.gz" >/dev/null
done
tenzir --bare-mode --console-verbosity=warning \
  "from_redis \"$queue\", url=env(\"REDIS_URL\"), db=2, count=2
   each {
     from_file string(\$this.message) {
       decompress_gzip
       read_pcap
     }
     this = decapsulate(this)
     file = string(\$this.message).file_name()
     summarize file, packets=count(), vlans=distinct(vlan?.outer?)
   }
   // \`each\` runs a subpipeline per file concurrently.
   sort file"
