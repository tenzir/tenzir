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

#include <caf/make_message.hpp>
#include <caf/message.hpp>

#include "vast/detail/string.hpp"
#include "vast/logger.hpp"

namespace vast {

command::command() : command(nullptr) {
  // nop
}

command::command(command* parent) : parent_(parent) {
  add_opt<bool>("help,h?", "prints the help text");
}

command::~command() {
  // nop
}

caf::message command::run(caf::actor_system& sys,
                          caf::config_value_map& options,
                          argument_iterator begin, argument_iterator end) {
  VAST_TRACE(VAST_ARG(std::string(name_)), VAST_ARG("args", begin, end),
             VAST_ARG(options));
  using caf::get_or;
  // Parse arguments for this command.
  auto [state, position] = opts_.parse(options, begin, end);
  bool has_subcommand;
  switch(state) {
    default:
      return wrap_error(parse_error(state, position, begin, end));
    case caf::pec::success:
      has_subcommand = false;
      break;
    case caf::pec::not_an_option:
      has_subcommand = position != end;
      break;
  }
  // Check for help option.
  if (get_or<bool>(options, "help", false)) {
    std::cerr << usage() << std::endl;
    return caf::none;
  }
  // Check for version option.
  if (get_or<bool>(options, "version", false)) {
    std::cerr << VAST_VERSION << std::endl;
    return caf::none;
  }
  // Check whether the options allow for further processing.
  if (auto err = proceed(sys, options, position, end))
    return wrap_error(std::move(err));
  // Invoke run_impl if no subcommand was defined.
  if (!has_subcommand)
    return run_impl(sys, options, position, end);
  // Consume CLI arguments if we have arguments but don't have subcommands.
  if (children_.empty())
    return run_impl(sys, options, position, end);
  // Dispatch to subcommand.
  // TODO: We need to copy the iterator here, because structured binding cannot
  //       be captured. Clang reports the error "reference to local binding
  //       'position' declared in enclosing function 'vast::command::run'" when
  //       trying to use the structured binding inside the lambda.
  //       See also: https://stackoverflow.com/questions/46114214. Remove this
  //       workaround when all supported compilers accept accessing structured
  //       bindings from lambdas.
  auto pos_cpy = position;
  auto i = std::find_if(children_.begin(), children_.end(),
                        [&](auto& x) { return x->name() == *pos_cpy; });
  if (i == children_.end())
    return wrap_error(unknown_subcommand_error(position, end));
  return (*i)->run(sys, options, position + 1, end);
}

caf::message command::run(caf::actor_system& sys, argument_iterator begin,
                          argument_iterator end) {
  caf::config_value_map options;
  return run(sys, options, begin, end);
}

std::string command::usage() const {
  std::stringstream result;
  result << opts_.help_text();
  if (!children_.empty()) {
    result << "\nsubcommands:\n";
    for (auto& child : children_)
      result << "  " << child->name() << "\n";
  }
  return result.str();
}

std::string command::full_name() const {
  std::string result;
  if (is_root())
    return result;
  result = parent_->full_name();
  if (!result.empty())
    result += ' ';
  result += name_;
  return result;
}

caf::error command::proceed(caf::actor_system&,
                            const caf::config_value_map& options,
                            argument_iterator begin, argument_iterator end) {
  VAST_UNUSED(options, begin, end);
  VAST_TRACE(VAST_ARG(std::string{name_}), VAST_ARG("args", begin, end),
             VAST_ARG(options));
  return caf::none;
}

caf::message command::run_impl(caf::actor_system&,
                               const caf::config_value_map& options,
                               argument_iterator begin, argument_iterator end) {
  VAST_UNUSED(options, begin, end);
  VAST_TRACE(VAST_ARG(std::string{name_}), VAST_ARG("args", begin, end),
             VAST_ARG(options));
  usage();
  return caf::none;
}

caf::message command::wrap_error(caf::error err) {
  return caf::make_message(std::move(err));
}

caf::error command::parse_error(caf::pec code, argument_iterator error_position,
                                argument_iterator begin,
                                argument_iterator end) const {
  std::stringstream result;
  result << "Failed to parse command" << "\n";
  result << "  Command: " << name() << " " << detail::join(begin, end, " ")
         << "\n";
  result << "  Description: " << to_string(code) << "\n";
  if (error_position != end)
    result << "  Position: " << *error_position << "";
  else
    result << "  Position: at the end";
  return make_error(ec::parse_error, result.str());
}

caf::error command::unknown_subcommand_error(argument_iterator error_position,
                                             argument_iterator end) const {
  VAST_ASSERT(error_position != end);
  auto& str = *error_position;
  std::string helptext;
  if (!str.empty() && str.front() == '-') {
    helptext = "invalid option: ";
    helptext += str;
  } else {
    helptext = "unknown command: ";
    if (!is_root()) {
      helptext += full_name();
      helptext += ' ';
    }
    helptext += str;
  }
  return make_error(ec::syntax_error, std::move(helptext));
}

} // namespace vast
