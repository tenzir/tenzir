#!/bin/bash

# A script that counts the number of partitions and segments in a given VAST database.
# Used to test the disk monitor in the vast integration suite.
# Since we can't specify '0' as high-water mark, we instead print the number of partitions
# plus 1 and use '1' as high-water mark.

np=`find "$1"/index/ -name '*.mdx' | wc -l`
echo $(($np + 1))
