# vast-regenerate

This is an developer-facing tool intended mainly debugging
and aiding rescue operations on broken databases. Only
use this if you know what you're doing, or if you don't care
about the contents of your database.

It has two modes, regenerating .mdx partition synopsis files
from existing partitions, and recreating a new index.bin
file from a list of partitions.

## Usage

See

    vast-regenerate --help
