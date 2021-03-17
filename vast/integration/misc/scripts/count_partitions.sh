#!/bin/sh

# A script that counts the number of partitions and segments in a given VAST database.
# Used to test the disk monitor in the vast integration suite.


ls -l "$1"/index/*.mdx | wc -l
