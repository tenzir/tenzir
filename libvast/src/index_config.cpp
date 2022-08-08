//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/index_config.hpp"

#include "vast/qualified_record_field.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <string_view>

namespace vast {

namespace {

bool is_target_applicable(const qualified_record_field& index_qf,
                          const std::string& target) {
  constexpr auto rule_prefix = std::string_view{":"};
  const auto is_type_rule = target.starts_with(rule_prefix);
  if (is_type_rule) {
    const auto type
      = std::string_view{cbegin(target) + rule_prefix.size(), cend(target)};
    return fmt::format("{}", index_qf.type()) == type;
  }

  return target == index_qf.name();
}

bool should_use_rule(const std::vector<std::string>& targets,
                     const qualified_record_field& index_qf) {
  return std::any_of(cbegin(targets), cend(targets),
                     [&index_qf](const auto& target) {
                       return is_target_applicable(index_qf, target);
                     });
}

} // namespace

bool should_create_dense_index(const qualified_record_field& index_qf,
                               const std::vector<index_config::rule>& rules) {
  for (const auto& rule : rules) {
    if (should_use_rule(rule.targets, index_qf)) {
      return rule.create_dense_index;
    }
  }

  return true;
}

} // namespace vast
