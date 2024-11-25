//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index_config.hpp"

#include "tenzir/qualified_record_field.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <string_view>

namespace tenzir {

namespace {

bool is_target_applicable(const qualified_record_field& index_qf,
                          std::string_view extractor) {
  // TODO: Replace the logic of this function with a simple call to `resolve` on
  // the typ with the extractor once we've unified type and field extractors.
  // This is unnecessarily complicated for now, but we don't currently have a
  // better API for it.
  const auto is_type_extractor = extractor.starts_with(':');
  if (!is_type_extractor)
    return index_qf.name() == extractor;
  const auto type_name = extractor.substr(1);
  if (type_name.empty())
    return false;
  // First check whether the type name of any of the type names it aliases match
  // the type extractor.
  const auto type = index_qf.type();
  for (const auto& name : type.names())
    if (type_name == name)
      return true;
  // If that's not the case, check whether the type name is the name of any of
  // the underlying basic type.
  const auto basic_type_name = [](const concrete_type auto& x) noexcept {
    return fmt::to_string(x);
  };
  return match(type, basic_type_name) == type_name;
}

bool should_use_rule(const std::vector<std::string>& targets,
                     const qualified_record_field& index_qf) {
  return std::any_of(cbegin(targets), cend(targets),
                     [&index_qf](const auto& target) {
                       return is_target_applicable(index_qf, target);
                     });
}

} // namespace

bool should_create_partition_index(
  const qualified_record_field& index_qf,
  const std::vector<index_config::rule>& rules) {
  for (const auto& rule : rules) {
    if (should_use_rule(rule.targets, index_qf))
      return rule.create_partition_index;
  }
  return true;
}

} // namespace tenzir
