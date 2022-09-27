//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "caf/detail/pretty_type_name.hpp"
#include "vast/error.hpp"
#include "vast/policy/merge_lists.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>
#include <fmt/format.h>

namespace vast::detail {

/// Merge settings of `src` into `dst`, overwriting existing values from `dst`
/// if necessary.
void merge_settings(const caf::settings& src, caf::settings& dst,
                    enum policy::merge_lists merge_lists);

/// Returns the amount of bytes specified by a config option.
/// * If the key has a string or integer key giving a size, use that.
/// * If the key doesn't exist, use the provided default value.
/// * If the key exists with a different type, return an error.
/// * If the key exists but cant be parsed as a byte size, return an error.
caf::expected<uint64_t>
get_bytesize(caf::settings opts, std::string_view key, uint64_t defval);

/// @brief Tries to extract a list value from config_value and convert it into
/// std::vector<T>.
/// @tparam T expected type of the list values.
/// @param cfg_value value which should be a list type of Ts.
/// @param error_message_on_list_extraction_failure error context of the error
/// in case the cfg_value is not a list type.
/// @return all list values extracted as Ts or error in 2 cases: the cfg_value
/// is not of a list type, any value in the list is not of type T.
template <class T>
caf::expected<std::vector<T>>
unpack_config_list_to_vector(const caf::config_value& cfg_value) {
  const auto* list = caf::get_if<caf::config_value::list>(&cfg_value);
  if (!list)
    return caf::make_error(ec::invalid_configuration, "failed to extract "
                                                      "config value as list");

  std::vector<T> ret;
  ret.reserve(list->size());
  for (const auto& e : *list) {
    const auto* val = caf::get_if<T>(&e);
    if (!val)
      return caf::make_error(
        ec::invalid_configuration,
        fmt::format("type mismatch while unpacking config list: expected {}, "
                    "got {}",
                    caf::detail::pretty_type_name(typeid(T)),
                    caf::detail::pretty_type_name(typeid(e))));
    ret.push_back(*val);
  }

  return ret;
}

/// @brief Tries to extract a list value from actor_system_config and convert it
/// into std::vector<T>.
/// @tparam T expected type of the list values.
/// @param cfg config which contains the list value.
/// @param cfg_list_key key holding the list value in the config.
/// @return all list values extracted as Ts or error in 3 cases: no value exists
/// under provided key in the config, the value in the config is not of a list
/// type, any value in the list is not of type T.
template <class T>
caf::expected<std::vector<T>>
unpack_config_list_to_vector(const caf::actor_system_config& cfg,
                             std::string_view cfg_list_key) {
  const auto& content = caf::content(cfg);
  const auto it = content.find(cfg_list_key);
  if (it == content.cend())
    return caf::make_error(
      ec::invalid_configuration,
      fmt::format("failed to find key '{}' in configuration", cfg_list_key));
  return unpack_config_list_to_vector<T>(it->second);
}

} // namespace vast::detail
