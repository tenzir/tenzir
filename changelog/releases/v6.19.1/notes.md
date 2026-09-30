This release fixes a data-loss regression in v6.19.0 when using custom disk-usage checks such as tenzir-df-percent. Upgrade affected nodes to prevent further data loss; this fix cannot restore data already deleted.

## 🐞 Bug fixes

### Data loss with percentage-based disk budgets

Custom disk-usage checks such as `tenzir-df-percent` once again preserve data when usage is below the configured watermarks. A regression in Tenzir v6.19.0 treated percentage thresholds as byte limits, which could delete existing data and newly ingested events even with ample free disk space. Eviction now checks disk usage again after each deletion batch. This fix cannot restore data already deleted.

*By @tobim.*
