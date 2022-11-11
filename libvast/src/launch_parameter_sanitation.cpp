//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/launch_parameter_sanitation.hpp"

#include "vast/detail/assert.hpp"
namespace vast::launch_parameter_sanitation {

namespace {

auto generate_default_value_for_argument_type(std::string_view type_name) {
  if (type_name.starts_with("uint") || type_name.starts_with("int")
      || type_name.starts_with("long")) {
    return "0";
  } else if (type_name == "timespan") {
    return "0s";
  } else if (type_name == "boolean") {
    return "false";
  } else if (type_name.starts_with("list")) {
    return "[]";
  }
  VAST_ASSERT(false && "option has type with no default value support");
  return "";
}
} // namespace

void sanitize_missing_arguments(std::vector<std::string>& arguments,
                                const vast::command& cmd) {
  auto dummy_options = caf::settings{};
  for (auto& argument : arguments) {
    if (!argument.starts_with("--")) {
      continue;
    }
    auto [state, _] = cmd.options.parse(dummy_options, {argument});
    if (state == caf::pec::not_an_option) {
      for (const auto& child_cmd : cmd.children) {
        sanitize_missing_arguments(arguments, *child_cmd);
      }
    } else if (state == caf::pec::missing_argument) {
      auto name = argument.substr(2, argument.length() - 3);
      auto option = cmd.options.cli_long_name_lookup(name);
      if (!option) {
        // something is wrong with the long name options:
        // reveal this during the actual parsing.
        continue;
      }
      auto option_type = option->type_name();
      auto options_type_default_val
        = generate_default_value_for_argument_type(option_type.data());
      argument.append(options_type_default_val);
    }
  }
}

} // namespace vast::launch_parameter_sanitation
