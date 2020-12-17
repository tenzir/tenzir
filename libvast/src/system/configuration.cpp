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

#include "vast/system/configuration.hpp"

#include "vast/concept/convertible/to.hpp"
#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/detail/add_message_types.hpp"
#include "vast/detail/append.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/process.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/format/writer_factory.hpp"
#include "vast/path.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/value_index.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>
#if VAST_USE_OPENCL
#  include <caf/opencl/manager.hpp>
#endif
#if VAST_USE_OPENSSL
#  include <caf/openssl/manager.hpp>
#endif

#include <algorithm>
#include <unordered_set>

namespace vast::system {

namespace {

template <class... Ts>
void initialize_factories() {
  (factory<Ts>::initialize(), ...);
}

} // namespace

configuration::configuration() {
  detail::add_message_types(*this);
  // Instead of the CAF-supplied `config_file_path`, we use our own
  // `config_files` variable in order to support multiple configuration files.
  auto add_configs = [&](auto&& dir) {
    config_files.emplace_back(dir / "vast.yml");
    config_files.emplace_back(dir / "vast.yaml");
  };
  if (const char* xdg_config_home = std::getenv("XDG_CONFIG_HOME"))
    add_configs(path{xdg_config_home} / "vast");
  else if (const char* home = std::getenv("HOME"))
    add_configs(path{home} / ".config" / "vast");
  add_configs(VAST_SYSCONFDIR / path{"vast"});
  // Remove all non-existent config files.
  config_files.erase(
    std::remove_if(config_files.begin(), config_files.end(),
                   [](auto&& p) { return !p.is_regular_file(); }),
    config_files.end());
  // Load I/O module.
  load<caf::io::middleman>();
  // GPU acceleration.
#if VAST_USE_OPENCL
  load<caf::opencl::manager>();
#endif
  initialize_factories<synopsis, table_slice_builder, value_index,
                       format::writer>();
}

caf::error configuration::parse(int argc, char** argv) {
  VAST_ASSERT(argc > 0);
  VAST_ASSERT(argv != nullptr);
  command_line.assign(argv + 1, argv + argc);
  // Translate -qqq to -vvv to the corresponding log levels. Note that the lhs
  // of the replacements may not be a valid option for any command.
  const auto replacements = std::vector<std::pair<std::string, std::string>>{
    {"-qqq", "--verbosity=quiet"}, {"-qq", "--verbosity=error"},
    {"-q", "--verbosity=warning"}, {"-v", "--verbosity=verbose"},
    {"-vv", "--verbosity=debug"},  {"-vvv", "--verbosity=trace"},
  };
  for (auto& option : command_line)
    for (const auto& [old, new_] : replacements)
      if (option == old)
        option = new_;
  // Move CAF options to the end of the command line, parse them, and then
  // remove them.
  auto is_vast_opt = [](auto& x) { return !detail::starts_with(x, "--caf."); };
  auto caf_opt = std::stable_partition(command_line.begin(), command_line.end(),
                                       is_vast_opt);
  std::vector<std::string> caf_args;
  std::move(caf_opt, command_line.end(), std::back_inserter(caf_args));
  command_line.erase(caf_opt, command_line.end());
  // If the user provided a config file on the command line, we attempt to
  // parse it last.
  for (auto& arg : command_line)
    if (detail::starts_with(arg, "--config="))
      config_files.push_back(arg.substr(9));
  // Check for multiple config files in directories.
  std::unordered_set<path> config_dirs;
  for (const auto& config : config_files) {
    if (!exists(config))
      return caf::make_error(ec::no_such_file,
                             "config file does not exist:", config.complete());
    auto dir = config.parent();
    if (config_dirs.count(dir))
      return caf::make_error(ec::parse_error, "found multiple config files in",
                             dir);
    else
      config_dirs.insert(std::move(dir));
  }
  // Parse and merge all configuration files.
  record merged_config;
  for (const auto& config : config_files) {
    if (exists(config)) {
      auto contents = load_contents(config);
      if (!contents)
        return contents.error();
      auto yaml = from_yaml(*contents);
      if (!yaml)
        return yaml.error();
      auto rec = caf::get_if<record>(&*yaml);
      if (!rec)
        return caf::make_error(ec::parse_error, "config file not a map of "
                                                "key-value pairs");
      merge(*rec, merged_config);
    }
  }
  // Flatten everything for simplicity.
  merged_config = flatten(merged_config);
  // Strip the caf. prefix from all keys.
  // TODO: Remove this after switching to CAF 0.18.
  for (auto& option : merged_config)
    if (auto& key = option.first;
        detail::starts_with(key.begin(), key.end(), "caf."))
      key.erase(0, 4);
  // Erase all null values because a caf::config_value has no such notion.
  for (auto i = merged_config.begin(); i != merged_config.end();) {
    if (caf::holds_alternative<caf::none_t>(i->second))
      i = merged_config.erase(i);
    else
      ++i;
  }
  // Convert to CAF-readable data structure.
  auto settings = to<caf::settings>(merged_config);
  if (!settings)
    return settings.error();
  // TODO: Revisit this after we are on CAF 0.18.
  // Helper function to parse a config_value with the type information
  // contained in an config_option. Because our YAML config only knows about
  // strings, but a config_option may require an atom, we have to use a
  // heuristic to see whether either type works.
  auto parse_config_value
    = [](const caf::config_option& opt,
         const caf::config_value val) -> caf::expected<caf::config_value> {
    // Hackish way to get a string representation that doesn't add double
    // quotes around the value.
    auto no_quote_stringify = detail::overload{
      [](const auto& x) { return caf::deep_to_string(x); },
      [](const std::string& x) { return x; },
    };
    auto str = caf::visit(no_quote_stringify, val);
    auto result = opt.parse(str);
    if (!result) {
      // We now try to parse strings as atom using a regex, since we get
      // recursive types like lists for free this way. A string-vs-atom type
      // clash is the only instance we currently cannot distinguish. Everything
      // else is a true type clash.
      // (With CAF 0.18, this heuristic will be obsolete.)
      str = detail::replace_all(std::move(str), "\"", "'");
      result = opt.parse(str);
      if (!result)
        return caf::make_error(ec::type_clash, "failed to parse config option",
                               caf::deep_to_string(opt.full_name()), str,
                               "expected",
                               caf::deep_to_string(opt.type_name()));
    }
    return result;
  };
  for (auto& [key, value] : *settings) {
    // We have flattened the YAML contents above, so dictionaries cannot occur.
    VAST_ASSERT(!caf::holds_alternative<caf::config_value::dictionary>(value));
    // Now this is incredibly ugly, but custom_options_ (a config_option_set)
    // is the only place that contains the valid type information that our
    // config file must abide to.
    if (auto option = custom_options_.qualified_name_lookup(key)) {
      if (auto x = parse_config_value(*option, value))
        put(content, key, std::move(*x));
      else
        return x.error();
    } else {
      // If the option is not relevant to CAF's custom options, we just store
      // the value directly in the content.
      put(content, key, value);
    }
  }
  // Try parsing all --caf.* settings. First, strip caf. prefix for the
  // CAF parser.
  for (auto& arg : caf_args)
    if (detail::starts_with(arg, "--caf."))
      arg.erase(2, 4);
  // We clear the config_file_path first so it does not use
  // caf-application.ini as fallback during actor_system_config::parse().
  config_file_path.clear();
  return actor_system_config::parse(std::move(caf_args));
}

} // namespace vast::system
