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

//int command::run(caf::actor_system& sys, XXoption_mapXX& options,
                 //argument_iterator begin, argument_iterator end) {
  //VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
  //// Split the arguments.
  //auto args = caf::message_builder{begin, end}.move_to_message();
  //auto [local_args, subcmd, subcmd_args] = separate_args(args);
  //begin += local_args.size();
  //// Parse arguments for this command.
  //auto res = local_args.extract_opts(opts_);
  //if (res.opts.count("help") != 0) {
    //std::cout << res.helptext << std::endl;
    //if (!nested_.empty()) {
      //std::cout << "\nSubcommands:\n";
      //for (auto& kvp : nested_)
        //std::cout << "  " << kvp.first << "\n";
    //}
    //std::cout << std::endl;
    //return EXIT_SUCCESS;
  //}
  //// Populate the map with our key/value pairs for all options.
  //for (auto& kvp : kvps_)
    //options.emplace(kvp());
  //// Check whether the options allow for further processing.
  //switch (proceed(sys, options, begin, end)) {
    //default:
        //// nop
        //break;
    //case stop_successful:
      //return EXIT_SUCCESS;
    //case stop_with_error:
      //return EXIT_FAILURE;
  //}
  //// Invoke run_impl if no subcommand was defined.
  //if (subcmd.empty()) {
    //VAST_ASSERT(subcmd_args.empty());
    //return run_impl(sys, options, begin, end);
  //}
  //// Consume CLI arguments if we have arguments but don't have subcommands.
  //if (nested_.empty()) {
    //return run_impl(sys, options, begin, end);
  //}
  //// Dispatch to subcommand.
  //auto i = nested_.find(subcmd);
  //if (i == nested_.end()) {
    //std::cerr << "no such command: " << full_name() << " " << subcmd
      //<< std::endl
      //<< std::endl;
    //usage();
    //return EXIT_FAILURE;
  //}
  //return i->second->run(sys, options, begin + 1, end);
//}

int command::run(caf::actor_system& sys, option_map& options,
                     argument_iterator begin, argument_iterator end) {

  // Parse arguments for this command.
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
  auto [state, position] = opts_.parse(options, begin, end);
  bool has_subcommand;
  switch(state) {
    default:
      // TODO: examine what went wrong and inform the user
      std::cerr << "something went wrong!!!" << std::endl;
      std::cerr << "parser state: " << static_cast<int>(state) << std::endl;
      std::cerr << usage() << std::endl;;
      return EXIT_FAILURE;
    case option_declaration_set::parse_state::successful:
      has_subcommand = false;
      break;
    case option_declaration_set::parse_state::begin_is_not_an_option:
      if (position == end)
        has_subcommand = false;
      has_subcommand = true;
      break;
  }
  // Check for help option.
  if (get_or<boolean>(options, "help", false)) {
    std::cerr << usage() << std::endl;;
    return EXIT_SUCCESS;
  }
  // Check whether the options allow for further processing.
  switch (proceed(sys, options, position, end)) {
    default:
        // nop
        break;
    case stop_successful:
      return EXIT_SUCCESS;
    case stop_with_error:
      return EXIT_FAILURE;
  }
  // Invoke run_impl if no subcommand was defined.
  if (!has_subcommand)
    return run_impl(sys, options, position, end);
  // Consume CLI arguments if we have arguments but don't have subcommands.
  if (nested_.empty())
    return run_impl(sys, options, position, end);
  // Dispatch to subcommand.
  auto i = nested_.find(*position);
  if (i == nested_.end()) {
    std::cerr << "no such command: " << full_name() << " " << *position
              << std::endl
              << std::endl;
    std::cerr << usage() << std::endl;;
    return EXIT_FAILURE;
  }
  return i->second->run(sys, options, position + 1, end);
}

//int command::run(caf::actor_system& sys, argument_iterator begin,
                 //argument_iterator end) {
  //XXoption_mapXX options;
  //return run(sys, options, begin, end);
//}

int command::run(caf::actor_system& sys, argument_iterator begin,
                 argument_iterator end) {
  option_map options;
  return run(sys, options, begin, end);
}

std::string command::usage() {
  std::stringstream result;
  result << opts_.usage() << "\n";
  result << "\nSubcommands:\n";
  for (auto& kvp : nested_)
    result << "  " << kvp.first << "\n";
  return result.str();
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

//command::proceed_result command::proceed(caf::actor_system&,
                                         //XXoption_mapXX& options,
                                         //argument_iterator begin,
                                         //argument_iterator end) {
  //VAST_UNUSED(options, begin, end);
  //VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
  //return proceed_ok;
//}

command::proceed_result command::proceed(caf::actor_system&,
                                         option_map& options,
                                         argument_iterator begin,
                                         argument_iterator end) {
  VAST_UNUSED(options, begin, end);
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
  return proceed_ok;
}

//int command::run_impl(caf::actor_system&, XXoption_mapXX& options,
                      //argument_iterator begin, argument_iterator end) {
  //VAST_UNUSED(options, begin, end);
  //VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
  //usage();
  //return EXIT_FAILURE;
//}

int command::run_impl(caf::actor_system&, option_map& options,
                      argument_iterator begin, argument_iterator end) {
  VAST_UNUSED(options, begin, end);
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
  usage();
  return EXIT_FAILURE;
}

//std::tuple<caf::message, std::string, caf::message>
//command::separate_args(const caf::message& args) {
  //auto arg = [&](size_t i) -> const std::string& {
    //VAST_ASSERT(args.match_element<std::string>(i));
    //return args.get_as<std::string>(i);
  //};
  //size_t pos = 0;
  //while (pos < args.size()) {
    //if (arg(pos).compare(0, 2, "--") == 0) {
      //// Simply skip over long options.
      //++pos;
    //} else if (arg(pos).compare(0, 1, "-") == 0) {
      //// We assume short options always have an argument.
      //// TODO: we could look into the argument instead of just assuming it
      ////       always take an argument.
      //pos += 2;
    //} else {
      //// Found the end of the options list.
      //return std::make_tuple(args.take(pos), arg(pos), args.drop(pos+ 1));
    //}
  //}
  //return std::make_tuple(args, "", caf::none);
//}

expected<void> command::add_opt(std::string_view name, std::string_view description, data default_value) {
  return opts_.add(name, description, std::move(default_value));
}

} // namespace vast
