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

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/settings.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/application.hpp"
#include "vast/system/start_command.hpp"

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

namespace vast {

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
    if (tname != "boolean") {
      lst += "=]";
      lst += " <";
      lst.insert(lst.end(), tname.begin(), tname.end());
      lst += '>';
    } else
      lst += ']';
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
/// `vast::parse` with the actual command for printing a human-readable
/// description of what went wrong.
void render_parse_error(const command& cmd, const invocation& inv,
                        const caf::error& err, std::ostream& os) {
  VAST_ASSERT(err != caf::none);
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
    system::render_error(cmd, err, os);
    os << std::endl;
  }
}

// Prints Markdown-formatted documentation for the taraget command.
void doctext(const command& cmd, std::ostream& out) {
  // TODO render with proper framing.
  out << std::left << cmd.documentation;
}

void manheader(std::ostream& out) {
  out << "% VAST(1)\n"
         "% Tenzir GmbH\n"
         "\n"
         "# NAME\n"
         "\n"
         "`vast` -- manage a VAST node\n"
         "\n"
         "# OVERVIEW\n"
         "\n"
         "This section describes the VAST system and its components from a "
         "user "
         "interaction point of view.\n"
         "\n";
}

void manfooter(std::ostream& out) {
  out << "# ISSUES\n"
         "\n"
         "If you encounter a bug or have suggestions for improvement, please "
         "file "
         "an issue at <http://vast.fail>.\n"
         "\n"
         "# SEE ALSO\n"
         "\n"
         "Visit <http://vast.io> for more information about VAST.\n";
}

// Prints Markdown-formatted documentation for the target command and all its
// subcommands with headers of increasing depth.
void mantext(const command& cmd, std::ostream& out,
             std::string::size_type depth) {
  auto header_prefix = std::string(depth, '#');
  out << header_prefix << ' ' << cmd.name << "\n\n";
  doctext(cmd, out);
  for (const auto& subcmd : cmd.children)
    if (subcmd->visible)
      mantext(*subcmd, out, depth + 1);
  out << '\n';
}

} // namespace

command::command(std::string_view name, std::string_view description,
                 std::string_view documentation, caf::config_option_set opts,
                 bool visible)
  : parent{nullptr},
    name{name},
    description{description},
    documentation{documentation},
    options{std::move(opts)},
    children{},
    visible{visible} {
}

command::command(std::string_view name, std::string_view description,
                 std::string_view documentation, command::opts_builder opts,
                 bool visible)
  : command(name, description, documentation, opts.finish(), visible) {
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

caf::config_option_set command::opts() {
  return caf::config_option_set{}.add<bool>("help,h?", "prints the help text");
}

command::opts_builder command::opts(std::string_view category) {
  return {category, opts()};
}

caf::error parse_impl(invocation& result, const command& cmd,
                      command::argument_iterator first,
                      command::argument_iterator last, const command** target) {
  using caf::get_or;
  VAST_TRACE(VAST_ARG(std::string(cmd.name)), VAST_ARG("args", first, last));
  // Parse arguments for this command.
  *target = &cmd;
  auto [state, position] = cmd.options.parse(result.options, first, last);
  result.assign(&cmd, position, last);
  if (get_or(result.options, "help", false))
    return caf::none;
  if (get_or(result.options, "documentation", false))
    return caf::none;
  if (get_or(result.options, "manual", false))
    return caf::none;
  bool has_subcommand;
  switch (state) {
    default:
      return make_error(ec::unrecognized_option, cmd.full_name(), *position,
                        state);
    case caf::pec::success:
      has_subcommand = false;
      break;
    case caf::pec::not_an_option:
      has_subcommand = position != last;
      break;
  }
  if (position != last && detail::starts_with(*position, "-"))
    return make_error(ec::unrecognized_option, cmd.full_name(), *position);
  // Check for help option.
  if (has_subcommand && *position == "help") {
    put(result.options, "help", true);
    return caf::none;
  }
  // Check for docomentation option.
  if (has_subcommand && *position == "documentation") {
    put(result.options, "documentation", true);
    return caf::none;
  }
  // Check for manual option.
  if (has_subcommand && *position == "manual") {
    put(result.options, "manual", true);
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
  //       'position' declared in enclosing function 'vast::command::run'" when
  //       trying to use the structured binding inside the lambda.
  //       See also: https://stackoverflow.com/questions/46114214. Remove this
  //       workaround when all supported compilers accept accessing structured
  //       bindings from lambdas.
  auto i = std::find_if(cmd.children.begin(), cmd.children.end(),
                        [p = position](auto& x) { return x->name == *p; });
  if (i == cmd.children.end())
    return make_error(ec::invalid_subcommand, cmd.full_name(), *position);
  return parse_impl(result, **i, position + 1, last, target);
}

caf::expected<invocation>
parse(const command& root, command::argument_iterator first,
      command::argument_iterator last) {
  invocation result;
  const command* target = nullptr;
  if (auto err = parse_impl(result, root, first, last, &target)) {
    render_parse_error(*target, result, err, std::cerr);
    return ec::silent;
  }
  if (get_or(result.options, "help", false)) {
    helptext(*target, std::cerr);
    return caf::no_error;
  }
  if (get_or(result.options, "documentation", false)) {
    doctext(*target, std::cerr);
    return caf::no_error;
  }
  if (get_or(result.options, "manual", false)) {
    manheader(std::cout);
    mantext(*target, std::cout, 3);
    manfooter(std::cout);
    return caf::no_error;
  }
  return result;
}

bool init_config(caf::actor_system_config& cfg, const invocation& from,
                 std::ostream& error_output) {
  // Disable file logging for all commands that don't start a node,
  // i.e., `vast start` and `vast -N ...`.
  if (!(from.name() == "start"
        || caf::get_or(from.options, "system.node", false)))
    cfg.set("logger.file-verbosity", caf::atom("quiet"));
  // Merge all CLI settings into the actor_system settings.
  detail::merge_settings(from.options, cfg.content);
  // Allow users to use `system.verbosity` to configure console verbosity.
  if (auto value = caf::get_if<caf::atom_value>(&from.options, "system."
                                                               "verbosity")) {
    // Verify user input.
    auto level = loglevel_to_int(caf::to_lowercase(*value), -1);
    if (level == -1) {
      error_output << "Invalid log level: " << to_string(*value) << ".\n"
                   << "Expected: quiet, error, warn, info, debug, or trace.\n";
      return false;
    }
    static constexpr std::string_view log_level_name[]
      = {"quiet", "",     "", "error",   "",      "", "warn", "",
         "",      "info", "", "verbose", "debug", "", "",     "trace"};
    if (level > VAST_LOG_LEVEL) {
      error_output << "Warning: desired log level " << to_string(*value)
                   << " exceeds the maximum log level for this software"
                      " version. Falling back to the maximum level ("
                   << log_level_name[VAST_LOG_LEVEL] << ").\n";
    }
    cfg.set("logger.console-verbosity", *value);
  }
  // Allow users to use `system.log-file` to set log filename
  if (auto fn = caf::get_if<std::string>(&cfg, "system.log-file"))
    cfg.set("logger.file-name", *fn);
  // Allow users to specify `verbose` log level.
  auto fixup_verbose_mode = [&](const std::string& option) {
    auto logopt = "logger." + option;
    if (auto verbosity = caf::get_if<caf::atom_value>(&cfg, logopt)) {
      put(cfg.content, "system." + option, *verbosity);
      if (*verbosity == caf::atom("verbose"))
        cfg.set(logopt, caf::atom("info"));
    }
  };
  fixup_verbose_mode("verbosity");
  fixup_verbose_mode("console-verbosity");
  fixup_verbose_mode("file-verbosity");
  // Adjust logger file name unless the user overrides the default.
  auto default_fn = caf::defaults::logger::file_name;
  auto file_verbosity = caf::get_or(cfg, "system.file-verbosity",
                                    defaults::logger::file_verbosity);
  if (caf::get_or(cfg, "logger.file-name", default_fn) == default_fn
      && loglevel_to_int(file_verbosity) > VAST_LOG_LEVEL_QUIET) {
    if (caf::get_if<std::string>(&cfg, "system.log-directory"))
      error_output << "Config option 'system.log-directory' is deprecated and"
                      " ignored, use 'system.log-file' instead\n";
    path log_dir
      = caf::get_or(cfg, "system.db-directory", defaults::system::db_directory);
    if (!exists(log_dir))
      if (auto err = mkdir(log_dir)) {
        error_output << "unable to create directory: " << log_dir.str() << ' '
                     << render(err) << '\n';
        return false;
      }
    // Store full path to the log file in config.
    auto log_file = log_dir / "server.log";
    cfg.set("logger.file-name", log_file.str());
  }
  return true;
}

caf::expected<caf::message> run(const invocation& inv, caf::actor_system& sys,
                                const command::factory& fact) {
  if (auto search_result = fact.find(inv.full_name);
      search_result != fact.end()) {
    // When coming from `main`, the original `sys.config()` was already merged
    // with the invocation options and this is a no-op, but when coming e.g.
    // from a remote_command we still need to do it here.
    auto merged_invocation = inv;
    merged_invocation.options = content(sys.config());
    detail::merge_settings(inv.options, merged_invocation.options);
    return std::invoke(search_result->second, merged_invocation, sys);
  }
  // No callback was registered for this command
  return make_error(ec::missing_subcommand, inv.full_name, "");
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

} // namespace vast
