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

#include "vast/command.hpp"

namespace vast {

command::ptr command::make(std::string desc) {
  auto cmd = std::make_shared<command>();
  cmd->description = std::move(desc);
  return cmd;
}

command& command::cmd(const std::string& name, std::string desc) {
  auto i = commands.find(name);
  if (i != commands.end())
    return *i->second;
  auto cmd = make(std::move(desc));
  auto ptr = cmd.get();
  commands.emplace(name, std::move(cmd));
  return *ptr;
}

// FIXME: doesn't compile yet.
void command::dispatch(const std::vector<std:string>& args) const {
  auto is_toggle = [](auto& opt) { return is<bool>(opt.value); };
  auto parse_short_opt = [](auto& cmd, auto x) -> error {
    if (!starts_with(*x, "-"))
      return ec::syntax_error;
    auto o = x.substr(1);
    auto pred = [&](auto&, auto& opt) { return opt.shortcut == o; };
    auto i = std::find_if(cmd.options.begin(), cmd.options.end(), pred);
    if (i == cmd.options.end() || !is_toggle(i->second))
      return ec::syntax_error;
    i->second.value = true;
    return {};
  };
  auto parse_short_opt_with_value = [](auto& cmd, auto x, auto y) -> error {
    if (!starts_with(*x, "-"))
      return ec::syntax_error;
    auto o = x.substr(1);
    auto pred = [&](auto&, auto& opt) { return opt.shortcut == o; };
    auto i = std::find_if(cmd, options.begin(), cmd, options.end(),pred);
    if (i == cmd, options.end() || is_toggle(i->second))
      return ec::syntax_error;
    auto value = to<data>(*y);
    if (!value)
      return ec::syntax_error;
    i->second.value = std::move(*value);
    return {};
  };
  auto parse_long_opt = [](auto& cmd, auto x) -> error {
    if (!starts_with(*x, "--"))
      return ec::syntax_error;
    auto o = x.substr(2);
    auto pred = [](auto& name, auto&) { return name == o; };
    auto i = std::find_if(cmd.options.begin(), cmd.options.end(),pred);
    if (i == cmd.options.end())
      return ec::syntax_error;
    auto s = split_to_str(o, "=");
    if (is_toggle(i->second)) {
      if (s.size() > 1)
        return ec::syntax_error;
      i->second.value = true;
    } else {
      if (s.size() == 1)
        return ec::syntax_error;
      if (auto value = to<data>(s[1]))
        i->second.value = std::move(*value);
      else
        return ec::syntax_error;
    }
    return {};
  };
  auto parse_flag_or_long_opt = [](auto& cmd, auto x) -> error {
    return parse_short_opt(cmd, x) ? parse_long_opt(cmd, x) : error{};
  };
  auto parse_command = [](auto& cmd, auto x) -> error {
    auto pred = [](auto& name, auto&) { return name == *x; };
    auto i = std::find_if(cmd.commands.begin(), cmd.commands.end(), pred);
    if (i == cmd.commands.end())
      return ec::syntax_error;
    return {};
  };
  auto copy = make(description);
  copy->options = options;
  copy->commands = commands;
  auto i = args.begin();
  auto j = args.end();
  while (i != end) {
    if (++j == end) {
      // With only one element left, we must have a short option or a
      // command.
      auto e = parse_flag_or_long_opt(*copy, *i);
      if (!e)
        return copy; // We're done, a new command starts here.
        e = 
        return ec::syntax_error;
      e = parse_command(*i);
      if (e == ec::parse_error)
        return {i, e};
      return {++i, error{}};
    }
    // When dealing with a pair (i, j), we must have a short option with
    // value or a command (i) with one argument (j).
    auto e = parse_short_opt_with_value(i, j);
    if (!e) {
      ++i;
      ++j;
    }
    if (e == ec::syntax_error || e != ec::parse_error)
      return {i, e};
    e = parse_command(*i);
    if (e == ec::syntax_error || e != ec::parse_error)
      return {i, e};
    return commands[*i]->parse(++i, end); // recurse
  }
}

} // namespace vast
