//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ocsf.hpp"

#include <optional>
#include <span>

namespace tenzir {

namespace {

struct ocsf_pair {
  ocsf_pair(int64_t id, std::string_view name) : id{id}, name{name} {
  }

  int64_t id;
  std::string_view name;
};

ocsf_pair category_map[] = {
#define X(id, name) {id, name},
#include "tenzir/ocsf_categories.inc"
#undef X
};

ocsf_pair class_map[] = {
#define X(id, name) {id, name},
#include "tenzir/ocsf_classes.inc"
#undef X
};

ocsf_pair type_map[] = {
#define X(id, name) {id, name},
#include "tenzir/ocsf_types.inc"
#undef X
};

auto name_to_id(std::span<const ocsf_pair> lookup, std::string_view key)
  -> std::optional<int64_t> {
  for (const auto& [id, name] : lookup) {
    if (key == name) {
      return id;
    }
  }
  return std::nullopt;
}

auto id_to_name(std::span<const ocsf_pair> lookup, int64_t key)
  -> std::optional<std::string_view> {
  for (const auto& [id, name] : lookup) {
    if (key == id) {
      return name;
    }
  }
  return std::nullopt;
}

} // namespace

auto ocsf_class_name(int64_t id) -> std::optional<std::string_view> {
  return id_to_name(class_map, id);
}

auto ocsf_class_uid(std::string_view name) -> std::optional<int64_t> {
  return name_to_id(class_map, name);
}

auto ocsf_category_name(int64_t id) -> std::optional<std::string_view> {
  return id_to_name(category_map, id);
}

auto ocsf_category_uid(std::string_view name) -> std::optional<int64_t> {
  return name_to_id(category_map, name);
}

auto ocsf_type_name(int64_t id) -> std::optional<std::string_view> {
  return id_to_name(type_map, id);
}

auto ocsf_type_uid(std::string_view name) -> std::optional<int64_t> {
  return name_to_id(type_map, name);
}

} // namespace tenzir
