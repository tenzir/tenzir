This release fixes the sigma operator to correctly load all rule files from a directory.

## 🐞 Bug fixes

### Fix sigma operator directory handling to load all rules

The `sigma` operator now correctly loads all rules when given a directory containing multiple Sigma rule files. Previously, only the last processed rule file would be retained because the rules collection was being cleared on every recursive directory traversal.

```tql
sigma "/path/to/sigma/rules"
```

All rules found in the directory and its subdirectories will now be loaded and used to match against input events.

*By @mavam and @claude in #5715.*
