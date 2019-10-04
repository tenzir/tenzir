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
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/error.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/system/start_command.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/defaults.hpp>
#include <caf/detail/log_level.hpp>
#include <caf/make_message.hpp>
#include <caf/message.hpp>
#include <caf/settings.hpp>

#include <functional>
#include <numeric>

namespace vast {

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
  return caf::config_option_set{}
    .add<bool>("help,h?", "prints the help text")
    .add<bool>("documentation?", "prints the Markdown-formatted documentation");
}

command::opts_builder command::opts(std::string_view category) {
  return {category, opts()};
}

command* command::add(std::string_view child_name,
                      caf::config_option_set child_options) {
  return children
    .emplace_back(
      new command{this, child_name, {}, {}, std::move(child_options), {}})
    .get();
}

caf::error parse_impl(command::invocation& result, const command& cmd,
                      command::argument_iterator first,
                      command::argument_iterator last, const command** target) {
  using caf::get_or;
  using caf::make_message;
  VAST_TRACE(VAST_ARG(std::string(cmd.name)), VAST_ARG("args", first, last));
  // Parse arguments for this command.
  *target = &cmd;
  auto [state, position] = cmd.options.parse(result.options, first, last);
  result.assign(&cmd, position, last);
  if (get_or(result.options, "help", false))
    return caf::none;
  if (get_or(result.options, "documentation", false))
    return caf::none;
  bool has_subcommand;
  switch (state) {
    default:
      return make_error(ec::unrecognized_option, cmd.full_name(), *position,
                        to_string(state));
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
  // Invoke cmd.run if no subcommand was defined.
  if (!has_subcommand) {
    // Commands without a run implementation require subcommands.
    if (command::factory.find(cmd.full_name()) == command::factory.end())
      return make_error(ec::missing_subcommand, cmd.full_name(), "");
    return caf::none;
  }
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

caf::expected<command::invocation>
parse(const command& root, command::argument_iterator first,
      command::argument_iterator last) {
  command::invocation result;
  const command* target;
  if (auto err = parse_impl(result, root, first, last, &target))
    return err;
  if (get_or(result.options, "help", false)) {
    helptext(*target, std::cerr);
    return caf::no_error;
  }
  if (get_or(result.options, "documentation", false)) {
    documentationtext(*target, std::cerr);
    return caf::no_error;
  }
  return result;
}

bool init_config(caf::actor_system_config& cfg, const command::invocation& from,
                 std::ostream& error_output) {
  // Utility function for merging settings.
  std::function<void(const caf::settings&, caf::settings&)> merge_settings;
  merge_settings = [&](const caf::settings& src, caf::settings& dst) {
    for (auto& [key, value] : src)
      if (caf::holds_alternative<caf::settings>(value)) {
        merge_settings(caf::get<caf::settings>(value),
                       dst[key].as_dictionary());
      } else {
        dst.insert_or_assign(key, value);
      }
  };
  // Merge all CLI settings into the actor_system settings.
  merge_settings(from.options, cfg.content);
  // Allow users to use `system.verbosity` to configure console verbosity.
  if (auto value = caf::get_if<caf::atom_value>(&from.options,
                                                "system.verbosity")) {
    // Verify user input.
    int level;
    using caf::atom_uint;
    switch (atom_uint(to_lowercase(*value))) {
      default:
        level = -1;
        break;
      case atom_uint("quiet"):
        level = CAF_LOG_LEVEL_QUIET;
        break;
      case atom_uint("error"):
        level = CAF_LOG_LEVEL_ERROR;
        break;
      case atom_uint("warn"):
        level = CAF_LOG_LEVEL_WARNING;
        break;
      case atom_uint("info"):
        level = CAF_LOG_LEVEL_INFO;
        break;
      case atom_uint("debug"):
        level = CAF_LOG_LEVEL_DEBUG;
        break;
      case atom_uint("trace"):
        level = CAF_LOG_LEVEL_TRACE;
    }
    if (level == -1) {
      error_output << "Invalid log level: " << to_string(*value) << ".\n"
                   << "Expected: quiet, error, warn, info, debug, or trace.\n";
      return false;
    }
    static constexpr std::string_view log_level_name[] = {"quiet", "", "",
                                                          "error", "", "",
                                                          "warn",  "", "",
                                                          "info",  "", "",
                                                          "debug", "", "",
                                                          "trace"};
    if (level > VAST_LOG_LEVEL) {
      error_output << "Warning: desired log level " << to_string(*value)
                   << " exceeds the maximum log level for this software"
                      " version. Falling back to the maximum level ("
                   << log_level_name[level] << ").\n";
    }
    cfg.set("logger.console-verbosity", *value);
  }
  // Adjust logger file name unless the user overrides the default.
  auto default_fn = caf::defaults::logger::file_name;
  if (caf::get_or(cfg, "logger.file-name", default_fn) == default_fn) {
    // Get proper directory path.
    path base_dir = get_or(cfg, "system.directory",
                           defaults::system::directory);
    auto log_dir = base_dir / "log";
    log_dir /= caf::deep_to_string(caf::make_timestamp()) + '#'
               + std::to_string(detail::process_id());
    // Create the log directory first, which we need to create the symlink
    // afterwards.
    if (!exists(log_dir))
      if (auto res = mkdir(log_dir); !res) {
        error_output << "Unable to create directory: " << log_dir.str() << "\n";
        return false;
      }
    // Create user-friendly symlink to current log directory.
    auto link_dir = log_dir.chop(-1) / "current";
    if (exists(link_dir))
      if (!rm(link_dir)) {
        error_output << "Cannot remove log symlink: " << link_dir.str() << "\n";
        return false;
      }
    auto src_dir = log_dir.trim(-1);
    if (auto err = create_symlink(src_dir, link_dir)) {
      error_output << "Cannot create symlink: " << src_dir.str() << " -> "
                   << link_dir.str() << "\n";
      return false;
    }
    // Store full path to the log file in config.
    auto log_file = log_dir / from.full_name + ".log";
    cfg.set("logger.file-name", log_file.str());
  }
  return true;
}

caf::expected<caf::message>
run(command::invocation& invocation, caf::actor_system& sys) {
  if (auto search_result = command::factory.find(invocation.full_name);
      search_result != command::factory.end())
    return std::invoke(search_result->second, invocation, sys);
  // No callback was registered for this command
  return make_error(ec::missing_subcommand, invocation.full_name, "");
}

const command& root(const command& cmd) {
  return cmd.parent == nullptr ? cmd : root(*cmd.parent);
}

const command* resolve(const command& cmd,
                       std::vector<std::string_view>::iterator position,
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
    // We print parameters in the form "[-h | -? | --help] <type>" (but we omit
    // the type for boolean). So, "[]" adds 2 characters, each short name adds
    // 5 characters with "-X | ", the long name gets the 2 character prefix
    // "--", and finally we add an extra space plus the type name.
    auto tname = y.type_name();
    auto tname_size = tname == "bool" ? 0u : tname.size() + 3;
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
    lst += ']';
    auto tname = opt.type_name();
    if (tname != "bool") {
      lst += " <";
      lst.insert(lst.end(), tname.begin(), tname.end());
      lst += '>';
    }
    out.width(fs);
    out << lst << "  " << opt.description() << '\n';
  }
}

// Prints the helptext for a command without children.
void flat_helptext(const command& cmd, std::ostream& out) {
  // A trivial command without parameters prints its name and description.
  if (cmd.options.empty()) {
    out << "usage: " << cmd.full_name() << "\n\n" << cmd.description << "\n\n";
    return;
  }
  // A command with parameters prints 1) its name, 2) a description, and 3) a
  // list of available parameters.
  out << "usage: " << cmd.full_name() << " [<parameters>]\n\n"
      << cmd.description << "\n\n";
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
        << "\n\n"
        << cmd.description << "\n\n";
    subcommand_helptext(cmd, out);
    return;
  }
  out << "usage: " << cmd.full_name() << " [<parameters>] <command>"
      << "\n\n"
      << cmd.description << "\n\n";
  parameters_helptext(cmd, out);
  out << '\n';
  subcommand_helptext(cmd, out);
}

} // namespace <anonymous>

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

void documentationtext(const command& cmd, std::ostream& out) {
  // TODO render with proper framing.
  out << std::left << cmd.documentation;
}

std::string documentationtext(const command& cmd) {
  std::ostringstream oss;
  documentationtext(cmd, oss);
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
                            const caf::settings& options,
                            argument_iterator begin, argument_iterator end) {
  VAST_UNUSED(options, begin, end);
  VAST_TRACE(VAST_ARG(std::string{name_}), VAST_ARG("args", begin, end),
             VAST_ARG(options));
  return caf::none;
}

caf::message command::run_impl(caf::actor_system&,
                               const caf::settings& options,
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
