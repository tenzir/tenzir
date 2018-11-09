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

#include <numeric>

#include <caf/make_message.hpp>
#include <caf/message.hpp>

#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

namespace vast {

caf::config_option_set command::opts() {
  return caf::config_option_set{}.add<bool>("help,h?", "prints the help text");
}

command* command::add(fun child_run, std::string_view child_name,
                      std::string_view child_description,
                      caf::config_option_set child_options) {
  return children
    .emplace_back(new command{this,
                              child_run,
                              child_name,
                              child_description,
                              std::move(child_options),
                              {}})
    .get();
}

caf::message run(const command& cmd, caf::actor_system& sys,
                 caf::config_value_map& options,
                 command::argument_iterator first,
                 command::argument_iterator last) {
  using caf::get_or;
  using caf::make_message;
  VAST_TRACE(VAST_ARG(std::string(name_)), VAST_ARG("args", first, last),
             VAST_ARG(options));
  // Parse arguments for this command.
  auto [state, position] = cmd.options.parse(options, first, last);
  bool has_subcommand;
  switch(state) {
    default:
      return make_message(make_error(ec::unrecognized_option, *position));
    case caf::pec::success:
      has_subcommand = false;
      break;
    case caf::pec::not_an_option:
      has_subcommand = position != last;
      break;
  }
  // Check whether we stopped at an unrecognized option.
  // TODO: replace `compare` with `starts_with` when switching to C++20
  if (position != last && position->compare(0, 1, "-") == 0)
    return make_message(make_error(ec::unrecognized_option, *position));
  // Check for help option.
  if (get_or<bool>(options, "help", false)) {
    helptext(cmd, std::cerr);
    return caf::none;
  }
  // Check for version option.
  if (get_or<bool>(options, "version", false)) {
    std::cerr << VAST_VERSION << std::endl;
    return caf::none;
  }
  // Invoke cmd.run if no subcommand was defined.
  if (!has_subcommand) {
    // Commands without a run implementation require subcommands.
    if (cmd.run == nullptr)
      return make_message(make_error(ec::missing_subcommand));
    return cmd.run(cmd, sys, options, position, last);
  }
  // Consume CLI arguments if we have arguments but don't have subcommands.
  if (cmd.children.empty())
    return cmd.run(cmd, sys, options, position, last);
  // Dispatch to subcommand.
  // TODO: We need to copy the iterator here, because structured binding cannot
  //       be captured. Clang reports the error "reference to local binding
  //       'position' declared in enclosing function 'vast::command::run'" when
  //       trying to use the structured binding inside the lambda.
  //       See also: https://stackoverflow.com/questions/46114214. Remove this
  //       workaround when all supported compilers accept accessing structured
  //       bindings from lambdas.
  auto i = std::find_if(cmd.children.begin(), cmd.children.end(),
                        [p = position](auto& x) { return x->name == *p; });
  if (i == cmd.children.end())
    return make_message(make_error(ec::invalid_subcommand, *position));
  return run(**i, sys, options, position + 1, last);
}

caf::message run(const command& cmd, caf::actor_system& sys,
                 command::argument_iterator first,
                 command::argument_iterator last) {
  caf::config_value_map options;
  return run(cmd, sys, options, first, last);
}

std::string full_name(const command& cmd) {
  std::string result{cmd.name};
  for (auto ptr = cmd.parent; ptr != nullptr; ptr = ptr->parent) {
    result.insert(result.begin(), ' ');
    result.insert(result.begin(), ptr->name.begin(), ptr->name.end());
  }
  return result;
}

namespace {

using std::accumulate;

// Returns the field size for printing all names in `xs`.
auto field_size(const command::children_list& xs) {
  return accumulate(xs.begin(), xs.end(), size_t{0}, [](size_t x, auto& y) {
    return std::max(x, y->name.size());
  });
}

// Returns the field size for printing all names in `xs`.
auto field_size(const caf::config_option_set& xs) {
  return accumulate(xs.begin(), xs.end(), size_t{0}, [](size_t x, auto& y) {
    // We print parameters in the form "[-h | -? | --help]". So, "[]" adds 2
    // characters, each short name adds 5 characters with "-X | " and the
    // long name finally gets the prefix "--".
    return std::max(x, 4 + (y.short_names().size() * 5) + y.long_name().size());
  });
}

void parameters_helptext(const command& cmd, std::ostream& out) {
  out << "parameters:\n";
  auto fs = field_size(cmd.options);
  for (auto& opt : cmd.options) {
    out << "  ";
    std::string lst = "[";
    for (auto ch : opt.short_names()) {
      lst += '-';
      lst += ch;
      lst += " | ";
    }
    lst += "--";
    lst.insert(lst.end(), opt.long_name().begin(), opt.long_name().end());
    lst += ']';
    out.width(fs);
    out << lst << "  " << opt.description() << '\n';
  }
}

// Prints the helptext for a command without children.
void flat_helptext(const command& cmd, std::ostream& out) {
  // A trivial command without parameters prints its name and description.
  if (cmd.options.empty()) {
    out << "usage: " << full_name(cmd) << "\n\n"
        << cmd.description << "\n\n";
    return;
  }
  // A command with parameters prints 1) its name, 2) a description, and 3) a
  // list of available parameters.
  out << "usage: " << full_name(cmd) << " [<parameters>]\n\n"
      << cmd.description << "\n\n";
  parameters_helptext(cmd, out);
}

void subcommand_helptext(const command& cmd, std::ostream& out) {
  out << "subcommands:\n";
  auto fs = field_size(cmd.children);
  for (auto& child : cmd.children) {
    out << "  ";
    out.width(fs);
    out << child->name << "  " << child->description << '\n';
  }
}

// Prints the helptext for a command without children.
void nested_helptext(const command& cmd, std::ostream& out) {
  // A trivial command without parameters prints name, description and
  // children.
  if (cmd.options.empty()) {
    out << "usage: " << full_name(cmd) << " <command>" << "\n\n"
        << cmd.description  << "\n\n";
    subcommand_helptext(cmd, out);
    return;
  }
  out << "usage: " << full_name(cmd) << " [<parameters>] <command>" << "\n\n"
      << cmd.description << "\n\n";
  parameters_helptext(cmd, out);
  out << '\n';
  subcommand_helptext(cmd, out);
}

} // namespace <anonymous>

void helptext(const command& cmd, std::ostream& out) {
  // Make sure fields are filled left-to-right.
  out << std::left;
  // Dispatch based on whether or nood cmd has children.
  if (cmd.children.empty())
    flat_helptext(cmd, out);
  else
    nested_helptext(cmd, out);
}

std::string helptext(const command& cmd) {
  std::ostringstream oss;
  helptext(cmd, oss);
  return oss.str();
}

/*

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
*/

} // namespace vast
