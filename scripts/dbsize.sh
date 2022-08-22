#!/bin/sh

df --output=pcent "$1" | tr -dc ‘0-9’
