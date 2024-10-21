//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/command.hpp"

#include "tenzir/application.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/die.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/defaults.hpp>
#include <caf/detail/log_level.hpp>
#include <caf/message.hpp>
#include <caf/settings.hpp>

#include <functional>
#include <iostream>
#include <numeric>
#include <string>

namespace tenzir {

namespace {

// Returns the field size for printing all names in `xs`.
auto field_size(const command::children_list& xs) {
  return std::accumulate(xs.begin(), xs.end(), size_t{0},
                         [](size_t x, auto& y) {
                           return std::max(x, y->name.size());
                         });
}

// Returns the field size for printing all names in `xs`.
auto field_size(const config_options& xs) {
  return std::accumulate(xs.begin(), xs.end(), size_t{0}, [](size_t x, auto& y) {
    // We print parameters in the form "[-h | -? | --help=] <type>" (but we omit
    // the type for boolean). So, "[=]" adds 3 characters, each short name adds
    // 5 characters with "-X | ", the long name gets the 2 character prefix
    // "--", and finally we add an extra space plus the type name.
    auto tname = y.type_name();
    auto tname_size = tname == "bool" ? 0u : tname.size() + 4;
    return std::max(x, 4 + (y.short_names().size() * 5) + y.long_name().size()
                         + tname_size);
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
    auto tname = opt.type_name();
    if (tname != "bool") {
      lst += "=]";
      lst += " <";
      lst.insert(lst.end(), tname.begin(), tname.end());
      lst += '>';
    } else {
      lst += ']';
    }
    out.width(fs);
    out << lst << "  " << opt.description() << '\n';
  }
}

// Prints the description for a command if there is any
void description(const command& cmd, std::ostream& out) {
  if (!cmd.description.empty())
    out << cmd.description << "\n\n";
}

// Prints the helptext for a command without children.
void flat_helptext(const command& cmd, std::ostream& out) {
  // A trivial command without parameters prints its name and description.
  if (cmd.options.empty()) {
    out << "usage: " << cmd.full_name() << "\n\n";
    description(cmd, out);
    return;
  }
  // A command with parameters prints 1) its name, 2) a description, and 3) a
  // list of available parameters.
  out << "usage: " << cmd.full_name() << " [<parameters>]\n\n";
  description(cmd, out);
  parameters_helptext(cmd, out);
}

void subcommand_helptext(const command& cmd, std::ostream& out) {
  out << "subcommands:\n";
  auto fs = field_size(cmd.children);
  for (auto& child : cmd.children) {
    if (child->visible) {
      out << "  ";
      out.width(fs);
      out << child->name << "  " << child->description << '\n';
    }
  }
}

// Prints the helptext for a command without children.
void nested_helptext(const command& cmd, std::ostream& out) {
  // A trivial command without parameters prints name, description and
  // children.
  if (cmd.options.empty()) {
    out << "usage: " << cmd.full_name() << " <command>"
        << "\n\n";
    description(cmd, out);
    subcommand_helptext(cmd, out);
    return;
  }
  out << "usage: " << cmd.full_name() << " [<parameters>] <command>"
      << "\n\n";
  parameters_helptext(cmd, out);
  out << '\n';
  subcommand_helptext(cmd, out);
}

// Two synchronized lines for highlighting text on the CLI.
using line_pair = std::pair<std::string, std::string>;

void append_plain(line_pair& dst, std::string_view x) {
  dst.first += x;
  dst.first += ' ';
  dst.second.insert(dst.second.end(), x.size() + 1, ' ');
}

void append_highlighted(line_pair& dst, std::string_view x) {
  dst.first += x;
  dst.first += ' ';
  dst.second += '^';
  dst.second.insert(dst.second.end(), x.size() - 1, '~');
  dst.second += ' ';
}

template <class ForwardIterator>
void append_plain(line_pair& dst, ForwardIterator first, ForwardIterator last) {
  for (; first != last; ++first)
    append_plain(dst, *first);
}

std::ostream& operator<<(std::ostream& os, const line_pair& x) {
  return os << '\n' << x.first << '\n' << x.second << '\n';
}

/// Iterates through the command hiearchy to correlate an error from
/// `tenzir::parse` with the actual command for printing a human-readable
/// description of what went wrong.
void render_parse_error(const command& cmd, const invocation& inv,
                        const caf::error& err, std::ostream& os) {
  TENZIR_ASSERT(err != caf::none);
  auto first = inv.arguments.begin();
  auto last = inv.arguments.end();
  auto position = first;
  // Convenience function for making a line pair with the current error
  // position highlighted in the second row.
  auto make_line_pair = [&] {
    line_pair lp;
    append_plain(lp, first, position);
    append_highlighted(lp, *position);
    append_plain(lp, position + 1, last);
    return lp;
  };
  if (err == ec::unrecognized_option) {
    os << "error: invalid option parameter";
    auto& context = err.context();
    if (context.match_elements<std::string, std::string, std::string>())
      os << " (" << context.get_as<std::string>(2) << ")";
    if (position != last)
      os << '\n' << make_line_pair() << '\n';
    helptext(cmd, os);
  } else if (err == ec::missing_subcommand) {
    os << "error: missing subcommand after " << inv.full_name << '\n';
    helptext(cmd, os);
  } else if (err == ec::invalid_subcommand) {
    os << "error: unrecognized subcommand" << '\n' << make_line_pair() << '\n';
    helptext(cmd, os);
  } else {
    render_error(cmd, err, os);
    os << std::endl;
  }
}

auto generate_default_value_for_argument_type(std::string_view type_name)
  -> std::string_view {
  if (type_name.starts_with("uint") || type_name.starts_with("int")
      || type_name.starts_with("long")) {
    return "0";
  } else if (type_name.find("timespan") != std::string_view::npos) {
    return "0s";
  } else if (type_name.starts_with("std::vector")) {
    return "[]";
  } else if (type_name.starts_with("dictionary")) {
    return "{}";
  }
  die(fmt::format("option has type '{}' with no default value support",
                  type_name));
}

void sanitize_long_form_argument(std::string& argument,
                                 const tenzir::command& cmd) {
  auto dummy_options = caf::settings{};
  auto [state, _] = cmd.options.parse(dummy_options, {argument});
  if (state == caf::pec::not_an_option) {
    for (const auto& child_cmd : cmd.children) {
      sanitize_long_form_argument(argument, *child_cmd);
    }
  } else if (state == caf::pec::missing_argument) {
    auto name = argument.substr(2, argument.length() - 3);
    auto option = cmd.options.cli_long_name_lookup(name);
    if (option) {
      auto option_type = option->type_name();
      auto options_type_default_val
        = generate_default_value_for_argument_type(option_type.data());
      argument.append(options_type_default_val);
    }
  }
}

auto sanitize_arguments(const command& root, command::argument_iterator first,
                        command::argument_iterator last) {
  std::vector<std::string> sanitized_arguments = {first, last};
  for (auto& argument : sanitized_arguments) {
    if (argument.starts_with("--")) {
      sanitize_long_form_argument(argument, root);
    }
  }
  return sanitized_arguments;
}
} // namespace

command::command(std::string_view name, std::string_view description,
                 config_options opts, bool visible)
  : parent{nullptr},
    name{name},
    description{description},
    options{std::move(opts)},
    children{},
    visible{visible} {
}

command::command(std::string_view name, std::string_view description,
                 command::opts_builder opts, bool visible)
  : command(name, description, opts.finish(), visible) {
}

std::string command::full_name() const {
  std::string result{name};
  for (auto ptr = parent; ptr != nullptr && ptr->parent != nullptr;
       ptr = ptr->parent) {
    if (!ptr->name.empty()) {
      result.insert(result.begin(), ' ');
      result.insert(result.begin(), ptr->name.begin(), ptr->name.end());
    }
  }
  return result;
}

config_options command::opts() {
  return config_options{}.add<bool>("help,h?", "prints the help text");
}

command::opts_builder command::opts(std::string_view category) {
  return {category, opts()};
}

caf::error parse_impl(invocation& result, const command& cmd,
                      command::argument_iterator first,
                      command::argument_iterator last, const command** target) {
  using caf::get_or;
  TENZIR_TRACE_SCOPE("{} {}", TENZIR_ARG(std::string(cmd.name)),
                     TENZIR_ARG("args", first, last));
  // Parse arguments for this command.
  *target = &cmd;
  auto [state, position] = cmd.options.parse(result.options, first, last);
  result.assign(&cmd, position, last);
  if (get_or(result.options, "help", false))
    return caf::none;
  bool has_subcommand;
  switch (state) {
    default: {
      std::string printable_position = "(unknown)";
      if (position != last)
        printable_position = *position;
      else if (first != last)
        printable_position = *first;
      return caf::make_error(ec::invalid_argument, cmd.full_name(),
                             printable_position, state);
    }
    case caf::pec::success:
      has_subcommand = false;
      break;
    case caf::pec::not_an_option:
      has_subcommand = position != last;
      break;
  }
  if (position != last && position->starts_with('-'))
    return caf::make_error(ec::unrecognized_option, cmd.full_name(), *position);
  // Check for help option.
  if (has_subcommand && *position == "help") {
    put(result.options, "help", true);
    return caf::none;
  }
  if (!has_subcommand)
    return caf::none;
  // Consume CLI arguments if we have arguments but don't have subcommands.
  if (cmd.children.empty())
    return caf::none;
  // Dispatch to subcommand.
  // TODO: We need to copy the iterator here, because structured binding cannot
  //       be captured. Clang reports the error "reference to local binding
  //       'position' declared in enclosing function 'tenzir::command::run'" when
  //       trying to use the structured binding inside the lambda.
  //       See also: https://stackoverflow.com/questions/46114214. Remove this
  //       workaround when all supported compilers accept accessing structured
  //       bindings from lambdas.
  auto i = std::find_if(cmd.children.begin(), cmd.children.end(),
                        [p = position](auto& x) { return x->name == *p; });
  if (i == cmd.children.end())
    return caf::make_error(ec::invalid_subcommand, cmd.full_name(), *position);
  return parse_impl(result, **i, position + 1, last, target);
}

caf::expected<invocation>
parse(const command& root, command::argument_iterator first,
      command::argument_iterator last) {
  auto sanitized_arguments = sanitize_arguments(root, first, last);
  invocation result;
  const command* target = nullptr;
  if (auto err = parse_impl(result, root, sanitized_arguments.begin(),
                            sanitized_arguments.end(), &target)) {
    render_parse_error(*target, result, err, std::cerr);
    return ec::silent;
  }
  if (get_or(result.options, "help", false)) {
    helptext(*target, std::cout);
    return {caf::error{}};
  }
  return result;
}

caf::expected<caf::message> run(const invocation& inv, caf::actor_system& sys,
                                const command::factory& fact) {
  if (auto search_result = fact.find(inv.full_name);
      search_result != fact.end()) {
    // When coming from `main`, the original `sys.config()` was already merged
    // with the invocation options and this is a no-op, but when coming e.g.
    // from a remote_command we still need to do it here. It is important that
    // we do not merge lists here to avoid accidental duplication of entries
    // from configuration shared between client and server.
    auto merged_invocation = inv;
    merged_invocation.options = content(sys.config());
    detail::merge_settings(inv.options, merged_invocation.options,
                           policy::merge_lists::no);
    return std::invoke(search_result->second, merged_invocation, sys);
  }
  // No callback was registered for this command
  return caf::make_error(ec::missing_subcommand, inv.full_name, "");
}

const command& root(const command& cmd) {
  return cmd.parent == nullptr ? cmd : root(*cmd.parent);
}

const command*
resolve(const command& cmd, std::vector<std::string_view>::iterator position,
        std::vector<std::string_view>::iterator end) {
  if (position == end)
    return &cmd;
  auto i = std::find_if(cmd.children.begin(), cmd.children.end(),
                        [&](auto& x) { return x->name == *position; });
  if (i == cmd.children.end())
    return nullptr;
  return resolve(**i, position + 1, end);
}

const command* resolve(const command& cmd, std::string_view name) {
  auto words = detail::split(name, " ");
  return resolve(cmd, words.begin(), words.end());
}

void helptext(const command& cmd, std::ostream& out) {
  // Make sure fields are filled left-to-right.
  out << std::left;
  // Dispatch based on whether or not cmd has visible children.
  auto visible = [](const auto& cmd) { return cmd->visible; };
  if (std::any_of(cmd.children.begin(), cmd.children.end(), visible))
    nested_helptext(cmd, out);
  else
    flat_helptext(cmd, out);
}

std::string helptext(const command& cmd) {
  std::ostringstream oss;
  helptext(cmd, oss);
  return oss.str();
}

} // namespace tenzir
