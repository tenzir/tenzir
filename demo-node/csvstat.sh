#!/bin/sh

echo "procs.waiting,procs.sleep,memory.virtual,memory.free,memory.buff,memory.cache,swap.in,swap.out,io.blocks_read,io.blocks.write,system.interrupts_per_second,system.context_switches_per_second,cpu.user,cpu.sys,cpu.idle,cpu.wait,cpu.stolen"
vmstat -a -n 1 | stdbuf -o 0 awk 'BEGIN{OFS=","} {$1=$1};NR>2;'
