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

#include <sstream>
#include <iomanip>

#include "vast/option_map.hpp"

#include "vast/concept/parseable/to.hpp"

#include "vast/detail/overload.hpp"

namespace vast {

option_declaration_set::option_declaration::option_declaration(
  std::string long_name, std::vector<char> short_names, std::string description,
  bool has_argument, data default_value)
    : long_name_(std::move(long_name)),
      short_names_(std::move(short_names)),
      description_(std::move(description)),
      has_argument_(has_argument),
      default_value_(std::move(default_value)) {
    // nop
}

expected<data> option_declaration_set::option_declaration::parse(
  const std::string& value) const {
  
  // FIXME: Use vast parser instead of parsing it by hand
  return visit(detail::overload(
    [&](const auto& /*arg*/) -> expected<data> {
      //using my_type = decltype(arg);
      //return to<my_type>(std::string{value});
      return make_error(ec::invalid_query, "");
    },
    [&](std::string) -> expected<data> { 
      return value;
    },
    [&](integer) -> expected<data> { 
      try {
        return std::stoll(value); 
      } catch (const std::invalid_argument& e) {
        return make_error(ec::unspecified, e.what());
      } catch (const std::out_of_range& e) {
        return make_error(ec::unspecified, e.what());
      }
    }), default_value_);
}

option_declaration_set::option_declaration_set() {
  add("help,h?", "print this text", false);
}

optional<const option_declaration_set::option_declaration&>
option_declaration_set::find(const std::string& name) const {
  if (auto it = long_opts_.find(name); it != long_opts_.end())
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
  // calculate the max size the argument column
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

optional<data> option_map::get(const std::string& name) const {
  if (auto x = xs_.find(name); x != xs_.end())
    return x->second;
  return {};
}

data option_map::get_or(const std::string& name, const data& default_value) const {
  if(auto x = get(name); x)
    return *x;
  return default_value;
}

void option_map::set(const std::string& name, const data& x) {
  xs_[name] = x;
}


expected<void> option_map::add(const std::string& name, const data& x) {
  if (auto it = xs_.find(name); it != xs_.end()) 
    return make_error(ec::unspecified, "name: " + name + " already exist");
  set(name, x);
  return {};
}

expected<void> option_declaration_set::add(const std::string& name,
                                           const std::string& desciption,
                                           data default_value) {
  // Parse short name and long name.
  std::string long_name;
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
  if (auto it = long_opts_.find(long_name); it != long_opts_.end())
    return make_error(ec::unspecified,
                      "long-name: " + long_name + " already in use");
  for (auto x : short_names)
    if (auto it = short_opts_.find(x); it != short_opts_.end())
      return make_error(ec::unspecified,
                        "short-name: " + to_string(x) + " already in use");
  // Update option_declaration_set.
  auto has_argument = visit(
    [](const auto& arg) {
      using arg_type = typename std::decay<decltype(arg)>::type;
      return !std::is_same<arg_type, bool>::value;
    }, default_value);
  auto option = std::make_shared<option_declaration>(
    std::move(long_name), std::move(short_names), std::move(desciption),
    has_argument, std::move(default_value));
  long_opts_.insert(std::make_pair(option->long_name(), option));
  for (auto x : option->short_names())
    short_opts_.insert(std::make_pair(x, option));
  return no_error;
}

std::pair<option_declaration_set::parse_result,
          option_declaration_set::argument_iterator>
option_declaration_set::parse(option_map& xs, argument_iterator begin,
                              argument_iterator end) const {
  // add all default values to the map
  for (auto& [long_name, x] : long_opts_) {
    // FIXME: the option help exists in every declaration and might cause and
    // error
    if (auto res = xs.add(long_name, x->default_value()); !res)
      if (long_name != "help")
        return std::make_pair(parse_result::option_already_exists, end);
  }
   //TODO: comment parse
  auto parse_argument = [&](auto idx, const auto& option, auto begin, auto end) {
    auto make_result = [](auto state, auto it, auto result) {
      return std::make_pair(std::make_pair(state, it), std::move(result));
    };
    if (begin == end)
      return make_result(parse_result::arg_declared_but_not_passed, begin,
                         data{});
    auto arg = begin->substr(idx);
    auto result = option->parse(arg);
    if (result)
      return make_result(parse_result::successful, ++begin,
                         std::move(*result));
    return make_result(parse_result::faild_to_parse_argument, begin, data{});
  };
  auto parse_short_option = [&](auto begin, auto end) {
    // extract short_name ("-s", "-s42", "-s"+"42")
    auto& x = *begin;
    auto indicator = 1u; // char count of "-"
    if (x.size() <= indicator)
      return std::make_pair(parse_result::name_not_declartion, begin);
    auto short_name = x[1];
    // find option
    auto it = short_opts_.find(short_name);
    if (it == short_opts_.end())
      return std::make_pair(parse_result::name_not_declartion, begin);
    auto& [_, option] = *it;
    auto& long_name = option->long_name();
    // parse argument if available
    if (option->has_argument()) {
      std::pair<parse_result, decltype(begin)> res;
      data argument;
      if (x.size() > indicator + 1) {
        std::tie(res, argument)
          = parse_argument(indicator + 1, option, begin, end);
      } else {
        std::tie(res, argument) = parse_argument(0, option, ++begin, end);
      }
      if (res.first != parse_result::successful)
        return res;
      xs.set(long_name, argument);
      return std::make_pair(parse_result::in_progress, res.second);
    } else {
      if (x.size() > indicator + 1)
        return std::make_pair(parse_result::arg_passed_but_not_declared, begin);
      xs.set(long_name, true);
      return std::make_pair(parse_result::in_progress, ++begin);
    }
  };
  auto parse_long_option = [&](auto begin, auto end) {
    // extract long_name ("--long_name", "--long_name=...")
    auto& x = *begin;
    auto idx = x.find('=');
    auto indicator = 2u; // char count of "--"
    auto long_name = x.substr(indicator, idx - indicator);
    // find option
    auto it = long_opts_.find(long_name);
    if (it == long_opts_.end())
      return std::make_pair(parse_result::name_not_declartion, begin);
    auto& [_, option] = *it;
    // parse argument if available
    if (option->has_argument()) {
      if (idx == std::string::npos)
        return std::make_pair(parse_result::arg_declared_but_not_passed, begin);
      auto [res, argument] = parse_argument(idx + 1, option, begin, end);
      if (res.first != parse_result::successful)
        return res;
      xs.set(long_name, argument);
      return std::make_pair(parse_result::in_progress, res.second);
    } else {
      if (idx != std::string::npos)
        return std::make_pair(parse_result::arg_passed_but_not_declared, begin);
      xs.set(long_name, true);
      return std::make_pair(parse_result::in_progress, ++begin);
    }
  };
  auto dispatch = [&](auto begin, auto end) {
    if (begin == end)
      return std::make_pair(parse_result::successful, end);
    if (detail::starts_with(*begin, "--"))
      return parse_long_option(begin, end);
    else if (detail::starts_with(*begin, "-"))
      return parse_short_option(begin, end);
    else
      return std::make_pair(parse_result::begin_is_not_an_option, begin);
  };
  auto [state, it] = dispatch(begin, end);
  while (state == parse_result::in_progress) {
    std::tie(state, it) = dispatch(it, end);
  }
  return std::make_pair(state, it);

}

} // namespace vast
