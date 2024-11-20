The contexts feature is now available in TQL2. It has undergone significant
changes to make use of TQL2's more powerful expressions. Contexts are shared
between TQL1 and TQL2 pipelines. All operators are grouped in the `context`
module, including the `enrich` and `show contexts` operators, which are now
called `context::enrich` and `context::list`, respectively. To create a new
context, use the `context::create_lookup_table`, `context::create_bloom_filter`,
or `context::create_geoip` operators.

Lookup table contexts now support separate create, write, and read timeouts via
the `create_timeout`, `write_timeout`, and `read_timeout` options, respectively.
The options are exclusive to contexts updated with TQL2's `context::update`
operator.
