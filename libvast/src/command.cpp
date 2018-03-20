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

#include "vast/logger.hpp"

namespace vast {

command::command() : parent_(nullptr) {
  // nop
}

command::command(command* parent, std::string_view name)
  : parent_(parent),
    name_(name) {
  // nop
}

command::~command() {
  // nop
}

int command::run(caf::actor_system& sys, opt_map& options, caf::message args) {
  VAST_TRACE(CAF_ARG(args));
  // Split the arguments.
  auto [local_args, subcmd, subcmd_args] = separate_args(args);
  // Parse arguments for this command.
  local_args.extract_opts(opts_);
  // Populate the map with our values.
  for (auto& f : get_opts_)
    options.emplace(f());
  // Invoke run_impl if no subcommand was defined.
  if (subcmd.empty()) {
    VAST_ASSERT(subcmd_args.empty());
    return run_impl(sys, options, std::move(local_args));
  }
  // Dispatch to subcommand.
  auto i = nested_.find(subcmd);
  if (i == nested_.end()) {
    std::cerr << "no such command: " << full_name() << " " << subcmd
              << std::endl
              << std::endl;
    usage();
    return EXIT_FAILURE;
  }
  return i->second->run(sys, options, std::move(subcmd_args));
}

int command::run(caf::actor_system& sys, caf::message args) {
  opt_map options;
  return run(sys, options, std::move(args));
}

void command::usage() {
  // nop
}

/*
command& command::cmd(const std::string& name, std::string desc) {
  auto i = commands.find(name);
  if (i != commands.end())
    return *i->second;
  auto cmd = make(std::move(desc));
  auto ptr = cmd.get();
  commands.emplace(name, std::move(cmd));
  return *ptr;
}
*/

/*
void command::dispatch(const std::vector<std::string>& args) const {
  auto is_toggle = [](auto& opt) { return is<bool>(opt.value); };
  auto parse_short_opt = [&](auto& cmd, auto x) -> error {
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
  auto parse_short_opt_with_value = [&](auto& cmd, auto x, auto y) -> error {
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
  auto parse_long_opt = [&](auto& cmd, auto x) -> error {
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
  auto parse_flag_or_long_opt = [&](auto& cmd, auto x) -> error {
    return parse_short_opt(cmd, x) ? parse_long_opt(cmd, x) : error{};
  };
  auto parse_command = [&](auto& cmd, auto x) -> error {
    auto pred = [&](auto& name, auto&) { return name == *x; };
    auto i = std::find_if(cmd.commands.begin(), cmd.commands.end(), pred);
    if (i == cmd.commands.end())
      return ec::syntax_error;
    return {};
  };
  auto copy = make(description);
  copy->options = options;
  copy->commands = commands;
  auto i = args.begin();
  auto j = i;
  auto end = args.end();
  while (i != end) {
    if (++j == end) {
      // With only one element left, we must have a short option or a
      // command.
      auto e = parse_flag_or_long_opt(*copy, *i);
      if (!e)
        return; // We're done, a new command starts here.
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
*/

std::string command::full_name() {
  std::string result;
  if (is_root())
    return result;
  result = parent_->full_name();
  if (!result.empty())
    result += ' ';
  result += name_;
  return result;
}

bool command::is_root() const noexcept {
  return parent_ == nullptr;
}

int command::run_impl(caf::actor_system&, opt_map&, caf::message) {
  usage();
  return EXIT_FAILURE;
}

std::tuple<caf::message, std::string, caf::message>
command::separate_args(const caf::message& args) {
  auto arg = [&](size_t i) -> const std::string& {
    VAST_ASSERT(args.match_element<std::string>(i));
    return args.get_as<std::string>(i);
  };
  size_t pos = 0;
  while (pos < args.size()) {
    if (arg(pos).compare(0, 2, "--") == 0) {
      // Simply skip over long options.
      ++pos;
    } else if (arg(pos).compare(0, 1, "-") == 0) {
      // We assume short options always have an argument.
      // TODO: we could look into the argument instead of just assuming it
      //       always take an argument.
      pos += 2;
    } else {
      // Found the end of the options list.
      return std::make_tuple(args.take(pos), arg(pos), args.drop(pos + 1));
    }
  }
  return std::make_tuple(args, "", caf::none);
}

} // namespace vast
