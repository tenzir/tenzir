//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ocsf.hpp"

namespace tenzir {

auto parse_ocsf_version(std::string_view name) -> std::optional<ocsf_version> {
#define X(other_name, version)                                                 \
  if (name == other_name) {                                                    \
    return ocsf_version::version;                                              \
  }
#include "tenzir/ocsf_versions.inc"
#undef X
  return std::nullopt;
}

auto ocsf_class_name(ocsf_version version, int64_t id)
  -> std::optional<std::string_view> {
#define X(other_version, other_id, name)                                       \
  if (version == ocsf_version::other_version and id == other_id) {             \
    return name;                                                               \
  }
#include "tenzir/ocsf_classes.inc"
#undef X
  return std::nullopt;
}

auto ocsf_class_uid(ocsf_version version, std::string_view name)
  -> std::optional<int64_t> {
#define X(other_version, id, other_name)                                       \
  if (version == ocsf_version::other_version and name == other_name) {         \
    return id;                                                                 \
  }
#include "tenzir/ocsf_classes.inc"
#undef X
  return std::nullopt;
}

auto ocsf_category_name(ocsf_version version, int64_t id)
  -> std::optional<std::string_view> {
#define X(other_version, other_id, name)                                       \
  if (version == ocsf_version::other_version and id == other_id) {             \
    return name;                                                               \
  }
#include "tenzir/ocsf_categories.inc"
#undef X
  return std::nullopt;
}

auto ocsf_category_uid(ocsf_version version, std::string_view name)
  -> std::optional<int64_t> {
#define X(other_version, id, other_name)                                       \
  if (version == ocsf_version::other_version and name == other_name) {         \
    return id;                                                                 \
  }
#include "tenzir/ocsf_categories.inc"
#undef X
  return std::nullopt;
}

auto ocsf_type_name(ocsf_version version, int64_t id)
  -> std::optional<std::string_view> {
#define X(other_version, other_id, name)                                       \
  if (version == ocsf_version::other_version and id == other_id) {             \
    return name;                                                               \
  }
#include "tenzir/ocsf_types.inc"
#undef X
  return std::nullopt;
}

auto ocsf_type_uid(ocsf_version version, std::string_view name)
  -> std::optional<int64_t> {
#define X(other_version, id, other_name)                                       \
  if (version == ocsf_version::other_version and name == other_name) {         \
    return id;                                                                 \
  }
#include "tenzir/ocsf_types.inc"
#undef X
  return std::nullopt;
}

} // namespace tenzir
