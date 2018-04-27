/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include <iomanip>

#include "vast/aliases.hpp"
#include "vast/error.hpp"
#include "vast/option_declaration_set.hpp"
#include "vast/option_map.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"

#include "vast/detail/overload.hpp"

namespace vast {

option_declaration_set::option_declaration::option_declaration(
  std::string_view long_name, std::vector<char> short_names,
  std::string_view description, data default_value)
  : long_name_(long_name),
    short_names_(std::move(short_names)),
    description_(description),
    default_value_(std::move(default_value)) {
  // nop
}

std::pair<option_declaration_set::parse_state, data>
option_declaration_set::option_declaration::parse(
  std::string_view value) const {
  auto result = visit(detail::overload(
    [&](const auto& arg) {
      using arg_type = std::decay_t<decltype(arg)>;
      auto x = to<arg_type>(value);
      if (!x)
        // FIXME: We lose valuable error informaton here.
        return std::make_pair(parse_state::failed_to_parse_argument,
                              default_value());
      return std::make_pair(parse_state::successful, data{*x});
    },
    // TODO: These overloads are nessesary as no respective parser exists at the 
    // moment. Remove me when possible.
    [&](const none&) {
      return std::make_pair(parse_state::type_not_parsebale,
                              default_value());
    },
    [&](const set&) {
      return std::make_pair(parse_state::type_not_parsebale,
                              default_value());
    },
    [&](const table&) {
      return std::make_pair(parse_state::type_not_parsebale,
                              default_value());
    },
    [&](const vector&) {
      return std::make_pair(parse_state::type_not_parsebale,
                              default_value());
    }), default_value_);
  return result; 
}

const std::string&
option_declaration_set::option_declaration::long_name() const {
  return long_name_;
}

const std::vector<char>&
option_declaration_set::option_declaration::short_names() const {
  return short_names_;
}

const std::string&
option_declaration_set::option_declaration::description() const {
  return description_;
}

bool option_declaration_set::option_declaration::has_argument() const {
  return visit(
    [](const auto& arg) {
      using arg_type = std::decay_t<decltype(arg)>;
      return !std::is_same<arg_type, bool>::value;
    }, default_value_);
}

const data& option_declaration_set::option_declaration::default_value() const {
  return default_value_;
}

option_declaration_set::option_declaration_set() {
  add("help,h?", "print this text", false);
}

optional<const option_declaration_set::option_declaration&>
option_declaration_set::find(std::string_view long_name) const {
  // TODO: Remove explicit conversion to a string
  // This requires to override *find* to support equivalent keys.
  if (auto it = long_opts_.find(std::string{long_name}); it != long_opts_.end())
    return *it->second;
  return {};
}

optional<const option_declaration_set::option_declaration&>
option_declaration_set::find(char short_name) const {
  if (auto it = short_opts_.find(short_name); it != short_opts_.end())
    return *it->second;
  return {};
}

std::string option_declaration_set::usage() const {
  //<--- argument ---> <---- desciption ---->
  //-w [--write] arg  : path to write events to
  auto build_argument = [](const option_declaration& x) {
    std::stringstream arg;
    auto& shorts = x.short_names();
    arg << "  "; 
    if (!shorts.empty()) {
      arg << "-" <<shorts[0];
      arg << " [";
    }
    if (shorts.size() > 1)
      for (auto i = 1u; i < shorts.size(); ++i)
        arg << "-" << shorts[i] << ",";
    arg << "--" << x.long_name();
    if (!shorts.empty())
      arg << "]";
    if (x.has_argument())
      arg << " arg";
    return arg.str();
  };
  // Calculate the max size the argument column
  std::vector<size_t> arg_sizes;
  std::transform(
    long_opts_.begin(), long_opts_.end(), std::back_inserter(arg_sizes),
    [&](const auto& x) { return build_argument(*x.second).size(); });
  auto help_column_width = *max_element(arg_sizes.begin(), arg_sizes.end());
  // create usage string
  std::stringstream res;
  res << "Allowed options:";
  for (auto& [_, x] : long_opts_) {
    res << "\n"
        << std::left << std::setw(help_column_width) << build_argument(*x)
        << " : " << x->description();
  }
  return res.str();
}

size_t option_declaration_set::size() const {
  return long_opts_.size();
}

expected<void> option_declaration_set::add(std::string_view name,
                                           std::string_view desciption,
                                           data default_value) {
  // Parse short and long name.
  std::string_view long_name;
  std::vector<char> short_names;
  if (auto idx = name.find(','); idx == std::string_view::npos) {
    long_name = name;
  } else {
    long_name = name.substr(0, idx);
    auto short_name = name.substr(idx + 1, name.size() - idx);
    short_names.reserve(short_name.size());
    for (auto x : short_name)
      short_names.emplace_back(x);
  }
  // Validate short and long name.
  if (long_name.empty())
    return make_error(ec::unspecified, "no long-name specified");
  // TODO: Remove explicit conversion to a string
  if (auto it = long_opts_.find(std::string{long_name}); it != long_opts_.end())
    return make_error(ec::unspecified, "long-name: " + std::string{long_name}
                                         + " already in use");
  for (auto x : short_names)
    if (auto it = short_opts_.find(x); it != short_opts_.end())
      return make_error(ec::unspecified,
                        "short-name: " + to_string(x) + " already in use");
  // Update option_declaration_set.
  auto option = std::make_shared<option_declaration>(
    long_name, std::move(short_names), desciption, std::move(default_value));
  long_opts_.insert(std::make_pair(option->long_name(), option));
  for (auto x : option->short_names())
    short_opts_.insert(std::make_pair(x, option));
  return no_error;
}

std::pair<option_declaration_set::parse_state,
          option_declaration_set::argument_iterator>
option_declaration_set::parse(option_map& xs, argument_iterator begin,
                              argument_iterator end) const {
  // Add all default values to the map.
  for (auto& [long_name, x] : long_opts_) {
    if (auto res = xs.add(long_name, x->default_value()); !res)
      if (long_name != "help")
        return std::make_pair(parse_state::option_already_exists, end);
  }
  auto parse_argument = [&](auto idx, const auto& option, auto begin, auto end) {
    auto make_result = [](auto state, auto it, auto result) {
      return std::make_pair(std::make_pair(state, it), std::move(result));
    };
    if (begin == end)
      return make_result(parse_state::arg_declared_but_not_passed, begin,
                         data{});
    auto arg = begin->substr(idx);
    auto [state, result] = option->parse(arg);
    if (state != parse_state::successful)
      return make_result(state, begin, data{});
    return make_result(state, ++begin, std::move(result));
  };
  auto parse_short_option = [&](auto begin, auto end) {
    // Extract the short name one of the fllowing strings: 
    // "-s", "-sXX", ["-s" "XX"]
    auto& x = *begin;
    auto indicator = 1u; // char count of "-"
    if (x.size() <= indicator)
      return std::make_pair(parse_state::name_not_declartion, begin);
    auto short_name = x[1];
    // Parse the argument if available.
    auto it = short_opts_.find(short_name);
    if (it == short_opts_.end())
      return std::make_pair(parse_state::name_not_declartion, begin);
    auto& [_, option] = *it;
    auto& long_name = option->long_name();
    // Parse the argument if available.
    if (option->has_argument()) {
      std::pair<parse_state, decltype(begin)> res;
      data argument;
      if (x.size() > indicator + 1) {
        std::tie(res, argument)
          = parse_argument(indicator + 1, option, begin, end);
      } else {
        std::tie(res, argument) = parse_argument(0, option, ++begin, end);
      }
      if (res.first != parse_state::successful)
        return res;
      xs.set(long_name, argument);
      return std::make_pair(parse_state::in_progress, res.second);
    } else {
      if (x.size() > indicator + 1)
        return std::make_pair(parse_state::arg_passed_but_not_declared, begin);
      xs.set(long_name, true);
      return std::make_pair(parse_state::in_progress, ++begin);
    }
  };
  auto parse_long_option = [&](auto begin, auto end) {
    // Extract the long_name from one of the following strings: 
    // "--long_name", "--long_name=XX".
    auto& x = *begin;
    auto idx = x.find('=');
    auto indicator = 2u; // char count of "--"
    auto long_name = x.substr(indicator, idx - indicator);
    // Searches for the releated option.
    auto it = long_opts_.find(long_name);
    if (it == long_opts_.end())
      return std::make_pair(parse_state::name_not_declartion, begin);
    auto& [_, option] = *it;
    // Parse the argument if available.
    if (option->has_argument()) {
      if (idx == std::string::npos)
        return std::make_pair(parse_state::arg_declared_but_not_passed, begin);
      auto [res, argument] = parse_argument(idx + 1, option, begin, end);
      if (res.first != parse_state::successful)
        return res;
      xs.set(long_name, argument);
      return std::make_pair(parse_state::in_progress, res.second);
    } else {
      if (idx != std::string::npos)
        return std::make_pair(parse_state::arg_passed_but_not_declared, begin);
      xs.set(long_name, true);
      return std::make_pair(parse_state::in_progress, ++begin);
    }
  };
  auto dispatch = [&](auto begin, auto end) {
    if (begin == end)
      return std::make_pair(parse_state::successful, end);
    if (detail::starts_with(*begin, "--"))
      return parse_long_option(begin, end);
    else if (detail::starts_with(*begin, "-"))
      return parse_short_option(begin, end);
    else
      return std::make_pair(parse_state::begin_is_not_an_option, begin);
  };
  auto [state, it] = dispatch(begin, end);
  while (state == parse_state::in_progress)
    std::tie(state, it) = dispatch(it, end);
  return std::make_pair(state, it);
}


} // namespace vast
