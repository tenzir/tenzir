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
#include "vast/type.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>
#if VAST_ENABLE_OPENSSL
#  include <caf/openssl/manager.hpp>
#endif

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <optional>
#include <unordered_set>

namespace vast::system {

namespace {

std::vector<std::filesystem::path> config_dirs_singleton = {};
std::vector<std::filesystem::path> loaded_config_files_singleton = {};

template <concrete_type T>
struct has_extension_type
  : std::is_base_of<arrow::ExtensionType, typename T::arrow_type> {};

/// Translates an environment variable to a config key. All keys follow the
/// pattern by PREFIX_SUFFIX, where PREFIX is the application-spefic prefix
/// that gets stripped. Thereafter, SUFFIX adheres to the following
/// substitution rules:
/// 1. A '_' translates into '-'
/// 2. A "__" translates into the record separator '.'
/// @pre `!prefix.empty()`
std::optional<std::string>
env_to_config(std::string_view key, std::string_view prefix = "VAST") {
  VAST_ASSERT(!prefix.empty());
  // PREFIX_X is the shortest allowed key.
  if (prefix.size() + 2 > key.size())
    return std::nullopt;
  if (!key.starts_with(prefix) || key[prefix.size()] != '_')
    return std::nullopt;
  auto suffix = key.substr(prefix.size() + 1);
  // From here on, "__" is the record separator and '_' translates into '-'.
  auto xs = detail::to_strings(detail::split(suffix, "__"));
  for (auto& x : xs)
    for (auto& c : x)
      c = (c == '_') ? '-' : tolower(c);
  return detail::join(xs, ".");
}

caf::expected<std::vector<std::filesystem::path>>
collect_config_files(std::vector<std::filesystem::path> dirs,
                     std::vector<std::string> cli_configs) {
  std::vector<std::filesystem::path> result;
  // First, go through all config file directories and gather config files
  // there. We populate the member variable `config_files` instead of
  // `config_file_path` in the base class so that we can support multiple
  // configuration files.
  for (auto&& dir : dirs) {
    // Support both *.yaml and *.yml extensions.
    auto conf_yaml = dir / "vast.yaml";
    auto conf_yml = dir / "vast.yml";
    std::error_code err{};
    const auto exists_conf_yaml = std::filesystem::exists(conf_yaml, err);
    if (err)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("failed to check if vast.yaml file "
                                         "exists in {}: {}",
                                         dir, err.message()));
    const auto exists_conf_yml = std::filesystem::exists(conf_yml, err);
    if (err)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("failed to check if vast.yml file "
                                         "exists in {}: {}",
                                         dir, err.message()));
    // We cannot decide which one to pick if we have two, so bail out.
    if (exists_conf_yaml && exists_conf_yml)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("detected both 'vast.yaml' and "
                                         "'vast.yml' files in {}",
                                         dir));
    if (exists_conf_yaml)
      result.emplace_back(std::move(conf_yaml));
    else if (exists_conf_yml)
      result.emplace_back(std::move(conf_yml));
  }
  // Second, consider command line and environment overrides. But only check
  // the environment if we don't have a config on the command line.
  if (cli_configs.empty())
    if (auto file = detail::getenv("VAST_CONFIG"))
      cli_configs.emplace_back(*file);
  for (const auto& file : cli_configs) {
    auto config_file = std::filesystem::path{file};
    std::error_code err{};
    if (std::filesystem::exists(config_file, err))
      result.push_back(std::move(config_file));
    else if (err)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("cannot find configuration file {}: "
                                         "{}",
                                         config_file, err.message()));
    else
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("cannot find configuration file {}",
                                         config_file));
  }
  return result;
}

caf::expected<record>
load_config_files(std::vector<std::filesystem::path> config_files) {
  loaded_config_files_singleton.clear();
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
  return merged_config;
}

/// Merges VAST environment variables into a configuration.
void merge_environment(record& config) {
  for (const auto& [key, value] : detail::environment())
    if (!value.empty())
      if (auto config_key = env_to_config(key)) {
        if (!config_key->starts_with("caf."))
          config_key->insert(0, "vast.");
        // These environment variables have been manually checked already.
        // Inserting them into the config would ignore higher-precedence values
        // from the command line.
        if (!(*config_key == "vast.config" || *config_key == "vast.plugins"
              || *config_key == "vast.plugin-dirs"
              || *config_key == "vast.bare-mode"
              || *config_key == "vast.config"))
          config[*config_key] = std::string{value};
      }
}

caf::expected<caf::settings> to_settings(record config) {
  // Pre-process our configuration so that it can be properly parsed later.
  // Strip the "caf." prefix from all keys.
  // TODO: Remove this after switching to CAF 0.18.
  for (auto& option : config)
    if (auto& key = option.first; std::string_view{key}.starts_with("caf."))
      key.erase(0, 4);
  // Erase all null values because a caf::config_value has no such notion.
  for (auto i = config.begin(); i != config.end();) {
    if (caf::holds_alternative<caf::none_t>(i->second))
      i = config.erase(i);
    else
      ++i;
  }
  return to<caf::settings>(config);
}

void populate_config_dirs() {
  config_dirs_singleton.clear();
  if (auto xdg_config_home = detail::getenv("XDG_CONFIG_HOME"))
    config_dirs_singleton.push_back(std::filesystem::path{*xdg_config_home}
                                    / "vast");
  else if (auto home = detail::getenv("HOME"))
    config_dirs_singleton.push_back(std::filesystem::path{*home} / ".config"
                                    / "vast");
  config_dirs_singleton.push_back(detail::install_configdir());
}

} // namespace

const std::vector<std::filesystem::path>& config_dirs() {
  return config_dirs_singleton;
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
  // Register Arrow extension types.
  auto register_extension_types =
    []<concrete_type... Ts>(caf::detail::type_list<Ts...>) {
    (static_cast<void>(Ts::arrow_type::register_extension()), ...);
  };
  register_extension_types(
    caf::detail::tl_filter_t<concrete_types, has_extension_type>{});
}

caf::error configuration::parse(int argc, char** argv) {
  // The main objective of this function is to parse the command line and put
  // it into the actor_system_config instance (`content`), which components
  // throughout VAST query to find out the application settings. This process
  // has several sequencing intricacies because it also loads configuration
  // files and considers environment variables while parsing the command line.
  //
  // A major issue is that we have to use caf::settings and cannot use
  // vast::record as unified representation, exacerbating the complexity of
  // this function. (We have plans to switch to a single, unified
  // representations.)
  //
  // When reviewing this function, it's important to keep the parsing
  // precedence in mind:
  //
  // 1. CLI arguments
  // 2. Environment variables
  // 3. Config files
  // 4. Defaults
  VAST_ASSERT(argc > 0);
  VAST_ASSERT(argv != nullptr);
  command_line.assign(argv + 1, argv + argc);
  // Translate -qqq to -vvv to the corresponding log levels. Note that the lhs
  // of the replacements may not be a valid option for any command.
  const auto replacements = std::vector<std::pair<std::string, std::string>>{
    {"-qqq", "--console-verbosity=quiet"},
    {"-qq", "--console-verbosity=error"},
    {"-q", "--console-verbosity=warning"},
    {"-v", "--console-verbosity=verbose"},
    {"-vv", "--console-verbosity=debug"},
    {"-vvv", "--console-verbosity=trace"},
  };
  for (auto& option : command_line)
    for (const auto& [old, new_] : replacements)
      if (option == old)
        option = new_;
  // Remove CAF options from the command line; we'll parse them at the very
  // end.
  auto is_vast_opt = [](const auto& x) {
    return !x.starts_with("--caf.");
  };
  auto caf_opt = std::stable_partition(command_line.begin(), command_line.end(),
                                       is_vast_opt);
  std::vector<std::string> caf_args;
  std::move(caf_opt, command_line.end(), std::back_inserter(caf_args));
  for (auto& arg : caf_args)
    arg.erase(2, 4); // Strip --caf. prefix
  command_line.erase(caf_opt, command_line.end());
  // Do not use builtin config directories in "bare mode".
  if (auto it
      = std::find(command_line.begin(), command_line.end(), "--bare-mode");
      it != command_line.end())
    caf::put(content, "vast.bare-mode", true);
  else if (auto vast_bare_mode = detail::getenv("VAST_BARE_MODE"))
    if (*vast_bare_mode == "true")
      caf::put(content, "vast.bare-mode", true);
  if (!caf::get_or(content, "vast.bare-mode", false))
    populate_config_dirs();
  // Detect when plugins or plugin-dirs are specified on the command line.
  // This needs to happen before the regular parsing of the command line
  // since plugins may add additional commands.
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
  // If there are no plugin options on the command line, look at the
  // corresponding evironment variables VAST_PLUGIN_DIRS and VAST_PLUGINS.
  if (auto vast_plugin_dirs = detail::getenv("VAST_PLUGIN_DIRS")) {
    auto cli_plugin_dirs
      = caf::get_or(content, "vast.plugin-dirs", std::vector<std::string>{});
    if (cli_plugin_dirs.empty()) {
      for (auto&& dir : detail::split(*vast_plugin_dirs, ":"))
        cli_plugin_dirs.emplace_back(std::move(dir));
      caf::put(content, "vast.plugin-dirs", std::move(cli_plugin_dirs));
    }
  }
  if (auto vast_plugins = detail::getenv("VAST_PLUGINS")) {
    auto cli_plugins
      = caf::get_or(content, "vast.plugins", std::vector<std::string>{});
    if (cli_plugins.empty()) {
      for (auto&& plugin : detail::split(*vast_plugins, ","))
        cli_plugins.emplace_back(std::move(plugin));
      caf::put(content, "vast.plugins", std::move(cli_plugins));
    }
  }
  // Gather and parse all to-be-considered configuration files.
  std::vector<std::string> cli_configs;
  for (auto& arg : command_line)
    if (arg.starts_with("--config="))
      cli_configs.push_back(arg.substr(9));
  if (auto configs = collect_config_files(config_dirs(), cli_configs))
    config_files = std::move(*configs);
  else
    return configs.error();
  auto config = load_config_files(config_files);
  if (!config)
    return config.error();
  *config = flatten(*config);
  merge_environment(*config);
  // From here on, we go into CAF land with the goal to put the configuration
  // into the members of this actor_system_config instance.
  auto settings = to_settings(std::move(*config));
  if (!settings)
    return settings.error();
  if (auto err = embed_config(*settings))
    return err;
  // Now parse all CAF options from the command line. Prior to doing so, we
  // clear the config_file_path first so it does not use caf-application.ini as
  // fallback during actor_system_config::parse().
  config_file_path.clear();
  auto result = actor_system_config::parse(std::move(caf_args));
  // Load OpenSSL last because it uses the parsed configuration.
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

caf::error configuration::embed_config(const caf::settings& settings) {
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
  for (auto& [key, value] : settings) {
    // This needs to be flattened so dictionaries cannot occur.
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
  return caf::none;
}

} // namespace vast::system
