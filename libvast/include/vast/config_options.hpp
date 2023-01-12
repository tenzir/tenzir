//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/string.hpp"

#include <caf/config_option_set.hpp>
#include <caf/string_view.hpp>
#include <fmt/format.h>

#include <unordered_set>

namespace vast {

/// A collection of configuration options that parses CLI arguments into a
/// caf::settings object. Each configuration option consists of the option type
/// (e.g. string, int, list), the name (e.g. console-verbosity) a category (e.g.
/// vast or global if not specified) and a description.
class config_options {
private:
  template <class T>
  struct is_std_vec : std::false_type {};

  template <class T>
  struct is_std_vec<std::vector<T>> : std::true_type {};

public:
  /// An iterator over CLI arguments.
  using argument_iterator = caf::config_option_set::argument_iterator;
  using parse_result = caf::config_option_set::parse_result;

  template <class OptionType>
  config_options& add(caf::string_view category, caf::string_view name,
                      caf::string_view description) {
    data_.add<OptionType>(category, name, description);
    return *this;
  }

  template <class OptionType>
    requires(is_std_vec<OptionType>::value)
  config_options& add(caf::string_view category, caf::string_view name,
                      caf::string_view description) {
    list_options_.insert(to_string(name));
    data_.add<OptionType>(category, name, description);
    return *this;
  }

  template <class T>
  config_options& add(caf::string_view name, caf::string_view description) {
    data_.add<T>(name, description);
    return *this;
  }

  template <class OptionType>
    requires(is_std_vec<OptionType>::value)
  config_options& add(caf::string_view name, caf::string_view description) {
    list_options_.insert(to_string(name));
    data_.add<OptionType>(name, description);
    return *this;
  }

  /// Parses a given args as CLI arguments into config.
  /// For example: adding an option: add<integer>("thread-count", "Number of
  /// threads to run algorithm") and having --thread-count=10 in one of the args
  /// would result in caf::settings having a caf::config_value under
  /// thread-count key with a value of 10
  parse_result
  parse(caf::settings& config, const std::vector<std::string>& args) const;

  /// Parses a given args range (first, last) as CLI arguments into config.
  parse_result parse(caf::settings& config, argument_iterator first,
                     argument_iterator last) const;

  auto begin() const noexcept {
    return data_.begin();
  }

  auto end() const noexcept {
    return data_.end();
  }

  bool empty() const noexcept {
    return data_.empty();
  }

  /// Returns the first caf::config_option that matches the CLI name.
  auto cli_long_name_lookup(caf::string_view name) const {
    return data_.cli_long_name_lookup(name);
  }

private:
  caf::config_option_set data_;
  std::unordered_set<std::string> list_options_;
};

} // namespace vast
