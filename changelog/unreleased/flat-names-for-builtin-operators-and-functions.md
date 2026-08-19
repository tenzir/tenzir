---
title: Flat names for builtin operators and functions
type: change
authors:
  - mavam
created: 2026-08-18T14:55:38.60116Z
---

Builtin operators and functions no longer live in modules. Modules are now
reserved exclusively for packages, so every builtin entity that used a
module-qualified name now uses a flat name:

| Before | After |
| --- | --- |
| `ai::prompt` | `ai_prompt` |
| `context::create_bloom_filter` | `context_create_bloom_filter` |
| `context::create_geoip` | `context_create_geoip` |
| `context::create_lookup_table` | `context_create_lookup_table` |
| `context::enrich` | `context_enrich` |
| `context::erase` | `context_erase` |
| `context::inspect` | `context_inspect` |
| `context::list` | `context_list` |
| `context::load` | `context_load` |
| `context::lookup` | `context_lookup` |
| `context::remove` | `context_remove` |
| `context::reset` | `context_reset` |
| `context::save` | `context_save` |
| `context::update` | `context_update` |
| `ocsf::cast` | `ocsf_cast` |
| `ocsf::derive` | `ocsf_derive` |
| `ocsf::trim` | `ocsf_trim` |
| `ocsf::category_name` | `ocsf_category_name` |
| `ocsf::category_uid` | `ocsf_category_uid` |
| `ocsf::class_name` | `ocsf_class_name` |
| `ocsf::class_uid` | `ocsf_class_uid` |
| `ocsf::type_name` | `ocsf_type_name` |
| `ocsf::type_uid` | `ocsf_type_uid` |
| `package::add` | `package_add` |
| `package::list` | `package_list` |
| `package::remove` | `package_remove` |
| `pipeline::activity` | `pipeline_activity` |
| `pipeline::detach` | `pipeline_detach` |
| `pipeline::list` | `pipeline_list` |
| `pipeline::run` | `pipeline_run` |

Before:

```tql
context::enrich "feodo", key=src_ip
```

After:

```tql
context_enrich "feodo", key=src_ip
```

The old spellings keep working until the next major release: they resolve to
the new name and emit a deprecation warning, for example:

```
warning: `context::enrich` is deprecated
  = note: modules are reserved for packages
  = hint: use `context_enrich` instead
```

Package entities always take precedence over this compatibility path, so a
package can now use any module name, including `context`, `ocsf`, `package`,
`pipeline`, and `ai`, without its operators and functions being shadowed by
builtins. Entities that exist only under their module-qualified name, such as
the deprecated `ocsf::apply`, keep resolving through the same path.
