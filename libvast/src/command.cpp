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
  : parent_{parent},
    name_{name} {
  // nop
}

command::~command() {
  // nop
}

int command::run(caf::actor_system& sys, option_map& options,
                 const_iterator args_begin, const_iterator args_end) {
  CAF_LOG_TRACE(CAF_ARG(options));
  // Split the arguments.
  auto args = caf::message_builder{args_begin, args_end}.move_to_message();
  auto [local_args, subcmd, subcmd_args, consumed_args] = separate_args(args);
  // Note: ???????Consumed arguments 
  args_begin += consumed_args;
  // Parse arguments for this command.
  auto res = local_args.extract_opts(opts_);
  if (res.opts.count("help") != 0) {
    std::cout << res.helptext << std::endl;
    if (!nested_.empty()) {
      std::cout << "\nSubcommands:\n";
      for (auto& kvp : nested_)
        std::cout << "  " << kvp.first << "\n";
    }
    std::cout << std::endl;
    return EXIT_SUCCESS;
  }
  // Populate the map with our key/value pairs for all options.
  for (auto& kvp : kvps_)
    options.emplace(kvp());
  // Check whether the options allow for further processing.
  switch (proceed(sys, options, args_begin, args_end)) {
    default:
        // nop
        break;
    case stop_successful:
      return EXIT_SUCCESS;
    case stop_with_error:
      return EXIT_FAILURE;
  }
  // Invoke run_impl if no subcommand was defined.
  if (subcmd.empty()) {
    VAST_ASSERT(subcmd_args.empty());
    return run_impl(sys, options, args_begin, args_end);
  }
  // Consume CLI arguments if we have arguments but don't have subcommands.
  if (nested_.empty()) {
    return run_impl(sys, options, args_begin, args_end);
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
  return i->second->run(sys, options, args_begin + 1, args_end);
}

int command::run(caf::actor_system& sys, const_iterator args_begin,
                 const_iterator args_end) {
  option_map options;
  return run(sys, options, args_begin, args_end);
}

void command::usage() {
  // nop
}

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

std::string command::name() {
  return std::string{name_};
}

bool command::is_root() const noexcept {
  return parent_ == nullptr;
}

command::proceed_result command::proceed(caf::actor_system&,
                                         option_map& options, const_iterator,
                                         const_iterator) {
  CAF_LOG_TRACE(CAF_ARG(options));
  CAF_IGNORE_UNUSED(options);
  return proceed_ok;
}

int command::run_impl(caf::actor_system&, option_map& options,
                      const_iterator, const_iterator) {
  CAF_LOG_TRACE(CAF_ARG(options));
  CAF_IGNORE_UNUSED(options);
  usage();
  return EXIT_FAILURE;
}

std::tuple<caf::message, std::string, caf::message, size_t>
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
      return std::make_tuple(args.take(pos), arg(pos), args.drop(pos+ 1),
                             pos);
    }
  }
  return std::make_tuple(args, "", caf::none, args.size());
}

} // namespace vast
