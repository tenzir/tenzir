//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/config_options.hpp"

#include <fmt/ranges.h>

namespace tenzir {

namespace {

constexpr auto arg_value_seperator = std::string_view{"="};
constexpr auto option_prefix = std::string_view{"--"};

std::string
convert_to_caf_compatible_list_arg(const std::string& comma_separated_list_arg) {
  const auto separation_index
    = comma_separated_list_arg.find_first_of(arg_value_seperator);
  const auto begin = comma_separated_list_arg.begin();
  if (separation_index == std::string::npos)
    return {};
  const auto arg_start_it
    = begin + separation_index + arg_value_seperator.length();
  if (arg_start_it == comma_separated_list_arg.end())
    return comma_separated_list_arg + "[]";
  const auto arg_name = std::string_view{begin, begin + separation_index};
  const auto arg
    = std::string_view(arg_start_it, comma_separated_list_arg.end());
  const auto split_args = detail::split_escaped(arg, ",", "\\");
  if (arg.starts_with('"') && arg.ends_with('"'))
    return fmt::format("{}{}[{}]", arg_name, arg_value_seperator,
                       fmt::join(split_args, "\",\""));
  return fmt::format("{}{}[\"{}\"]", arg_name, arg_value_seperator,
                     fmt::join(split_args, "\",\""));
}
} // namespace

config_options::parse_result
config_options::parse(caf::settings& config, argument_iterator first,
                      argument_iterator last) const {
  const auto args = std::vector<std::string>{first, last};
  const auto result = parse(config, args);
  const auto arg_nr = std::distance(args.cbegin(), result.second);
  return {result.first, first + arg_nr};
}

config_options::parse_result
config_options::parse(caf::settings& config,
                      const std::vector<std::string>& args) const {
  auto args_copy = args;
  for (auto& arg : args_copy) {
    if (arg.starts_with(option_prefix)) {
      const auto option_name
        = arg.substr(0, arg.find_first_of(arg_value_seperator))
            .substr(option_prefix.length());
      if (list_options_.contains(option_name))
        arg = convert_to_caf_compatible_list_arg(arg);
    }
  }
  const auto result = data_.parse(config, args_copy);
  const auto arg_nr = std::distance(args_copy.cbegin(), result.second);
  return {result.first, args.begin() + arg_nr};
}

} // namespace tenzir
