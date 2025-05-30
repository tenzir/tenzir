We fixed a bug where the `export`, `metrics`, and `diagnostics` operators were
sometimes missing events from up to the last 30 seconds. In the Tenzir Platform,
this showed itself as a gap in activity sparkbars upon loading the page.
