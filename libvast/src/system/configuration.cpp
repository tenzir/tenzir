//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/configuration.hpp"

#include "vast/concept/convertible/to.hpp"
#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/detail/add_message_types.hpp"
#include "vast/detail/append.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/env.hpp"
#include "vast/detail/installdirs.hpp"
#include "vast/detail/load_contents.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>
#if VAST_ENABLE_OPENSSL
#  include <caf/openssl/manager.hpp>
#endif

#include <algorithm>
#include <filesystem>
#include <unordered_set>

namespace vast::system {

namespace {

std::vector<std::filesystem::path> loaded_config_files_singleton = {};

} // namespace

std::vector<std::filesystem::path>
config_dirs(const caf::actor_system_config& config) {
  const auto bare_mode = caf::get_or(config.content, "vast.bare-mode", false);
  if (bare_mode)
    return {};
  auto result = std::vector<std::filesystem::path>{};
  if (auto xdg_config_home = detail::locked_getenv("XDG_CONFIG_HOME"))
    result.push_back(std::filesystem::path{*xdg_config_home} / "vast");
  else if (auto home = detail::locked_getenv("HOME"))
    result.push_back(std::filesystem::path{*home} / ".config" / "vast");
  result.push_back(detail::install_configdir());
  return result;
}

const std::vector<std::filesystem::path>& loaded_config_files() {
  return loaded_config_files_singleton;
}

configuration::configuration() {
  detail::add_message_types(*this);
  // Load I/O module.
  load<caf::io::middleman>();
  // Initialize factories.
  factory<synopsis>::initialize();
  factory<table_slice_builder>::initialize();
  factory<value_index>::initialize();
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
  // Detect when running with --bare-mode, and remove the option from the
  // command line.
  if (auto it
      = std::find(command_line.begin(), command_line.end(), "--bare-mode");
      it != command_line.end()) {
    caf::put(content, "vast.bare-mode", true);
    command_line.erase(it);
  }
  // Detect when plugins or plugin-dirs are specified on the command line. This
  // needs to happen before the regular parsing of the command line since
  // plugins may add additional commands.
  auto is_not_plugin_opt = [](auto& x) {
    return !x.starts_with("--plugins=") && !x.starts_with("--plugin-dirs=");
  };
  auto plugin_opt = std::stable_partition(
    command_line.begin(), command_line.end(), is_not_plugin_opt);
  auto plugin_args = std::vector<std::string>{};
  std::move(plugin_opt, command_line.end(), std::back_inserter(plugin_args));
  command_line.erase(plugin_opt, command_line.end());
  auto plugin_opts
    = caf::config_option_set{}
        .add<std::vector<std::string>>("?vast", "plugin-dirs", "")
        .add<std::vector<std::string>>("?vast", "plugins", "");
  auto [ec, it] = plugin_opts.parse(content, plugin_args);
  VAST_ASSERT(ec == caf::pec::success);
  VAST_ASSERT(it == plugin_args.end());
  // Support specifying VAST_PLUGIN_DIRS on the environment.
  {
    auto cli_plugin_dirs
      = caf::get_or(content, "vast.plugin-dirs", std::vector<std::string>{});
    if (auto vast_plugin_directories = detail::locked_getenv( //
          "VAST_PLUGIN_DIRS")) {
      const auto env_plugin_dirs = detail::split(*vast_plugin_directories, ":");
      for (const auto& dir : env_plugin_dirs)
        cli_plugin_dirs.emplace_back(dir);
      if (!cli_plugin_dirs.empty())
        caf::put(content, "vast.plugin-dirs", std::move(cli_plugin_dirs));
    }
  }
  // Move CAF options to the end of the command line, parse them, and then
  // remove them.
  auto is_vast_opt = [](const auto& x) {
    return !x.starts_with("--caf.");
  };
  auto caf_opt = std::stable_partition(command_line.begin(), command_line.end(),
                                       is_vast_opt);
  std::vector<std::string> caf_args;
  std::move(caf_opt, command_line.end(), std::back_inserter(caf_args));
  command_line.erase(caf_opt, command_line.end());
  // Instead of the CAF-supplied `config_file_path`, we use our own
  // `config_files` variable in order to support multiple configuration files.
  auto add_configs = [&](std::filesystem::path&& dir) -> caf::error {
    auto conf_yaml = dir / "vast.yaml";
    auto conf_yml = dir / "vast.yml";
    std::error_code err{};
    const auto exists_conf_yaml = std::filesystem::exists(conf_yaml, err);
    const auto exists_conf_yml = std::filesystem::exists(conf_yml, err);
    if (err) {
      VAST_WARN("failed to check if vast.yaml file exists in {}: {}", dir,
                err.message());
      return caf::none;
    }
    if (exists_conf_yaml && exists_conf_yml)
      return caf::make_error(
        ec::invalid_configuration,
        "detected both 'vast.yaml' and 'vast.yml' files in " + dir.string());
    if (exists_conf_yaml)
      config_files.emplace_back(std::move(conf_yaml));
    else if (exists_conf_yml)
      config_files.emplace_back(std::move(conf_yml));
    return caf::none;
  };
  for (auto&& config_dir : config_dirs(*this))
    if (auto err = add_configs(std::move(config_dir)))
      return err;
  // If the user provided a config file on the command line, we attempt to
  // parse it last.
  for (auto& arg : command_line) {
    if (arg.starts_with("--config=")) {
      std::error_code err{};
      if (auto config = std::filesystem::path{arg.substr(9)};
          std::filesystem::exists(config, err))
        config_files.push_back(std::move(config));
      else
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("cannot find configuration file {}: "
                                           "{}",
                                           config, err.message()));
    }
  }
  // Parse and merge all configuration files.
  record merged_config;
  for (const auto& config : config_files) {
    std::error_code err{};
    if (std::filesystem::exists(config, err)) {
      auto contents = detail::load_contents(config);
      if (!contents)
        return caf::make_error(ec::parse_error,
                               fmt::format("failed to read config file {}: {}",
                                           config, contents.error()));
      auto yaml = from_yaml(*contents);
      if (!yaml)
        return caf::make_error(ec::parse_error,
                               fmt::format("failed to read config file {}: {}",
                                           config, yaml.error()));
      // Skip empty config files.
      if (caf::holds_alternative<caf::none_t>(*yaml))
        continue;
      auto* rec = caf::get_if<record>(&*yaml);
      if (!rec)
        return caf::make_error(ec::parse_error,
                               fmt::format("failed to read config file {}: not "
                                           "a map of key-value pairs",
                                           config));
      merge(*rec, merged_config, policy::merge_lists::yes);
      loaded_config_files_singleton.push_back(config);
    }
  }
  // Flatten everything for simplicity.
  merged_config = flatten(merged_config);
  // Strip the caf. prefix from all keys.
  // TODO: Remove this after switching to CAF 0.18.
  for (auto& option : merged_config)
    if (auto& key = option.first; std::string_view{key}.starts_with("caf."))
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
         const caf::config_value& val) -> caf::expected<caf::config_value> {
    // Hackish way to get a string representation that doesn't add double
    // quotes around the value.
    auto no_quote_stringify = detail::overload{
      [](const auto& x) {
        return caf::deep_to_string(x);
      },
      [](const std::string& x) {
        return x;
      },
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
    if (const auto* option = custom_options_.qualified_name_lookup(key)) {
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
  // If the user specifies a VAST_ENDPOINT, potentially use it. Precedence:
  // 1. CLI argument
  // 2. Environment variables
  // 3. Config files
  // 4. Defaults
  if (auto vast_endpoint_env = detail::locked_getenv("VAST_ENDPOINT"))
    caf::put(content, "vast.endpoint", *vast_endpoint_env);
  // Try parsing all --caf.* settings. First, strip caf. prefix for the
  // CAF parser.
  for (auto& arg : caf_args)
    if (arg.starts_with("--caf."))
      arg.erase(2, 4);
  // We clear the config_file_path first so it does not use
  // caf-application.ini as fallback during actor_system_config::parse().
  config_file_path.clear();
  auto result = actor_system_config::parse(std::move(caf_args));
  // Load OpenSSL module if configured to do so.
#if VAST_ENABLE_OPENSSL
  const auto use_encryption
    = !openssl_certificate.empty() || !openssl_key.empty()
      || !openssl_passphrase.empty() || !openssl_capath.empty()
      || !openssl_cafile.empty();
  if (use_encryption)
    load<caf::openssl::manager>();
#endif // VAST_ENABLE_OPENSSL
  return result;
}

} // namespace vast::system
