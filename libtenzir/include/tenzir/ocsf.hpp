//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace tenzir {

enum class ocsf_version {
#define X(name, identifier) identifier,
#include "tenzir/ocsf_versions.inc"
#undef X
};

auto parse_ocsf_version(std::string_view name) -> std::optional<ocsf_version>;

auto ocsf_class_name(ocsf_version version, int64_t id)
  -> std::optional<std::string_view>;
auto ocsf_class_uid(ocsf_version version, std::string_view name)
  -> std::optional<int64_t>;

auto ocsf_category_name(ocsf_version version, int64_t id)
  -> std::optional<std::string_view>;
auto ocsf_category_uid(ocsf_version version, std::string_view name)
  -> std::optional<int64_t>;

// TODO: Types with a trailing 99 belong to the "Other" category, in which case
// the name is supposed to contain a custom value instead of just using "Other".
auto ocsf_type_name(ocsf_version version, int64_t id)
  -> std::optional<std::string_view>;
auto ocsf_type_uid(ocsf_version version, std::string_view name)
  -> std::optional<int64_t>;

} // namespace tenzir
