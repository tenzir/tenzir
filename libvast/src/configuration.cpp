//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/configuration.hpp"

#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/config.hpp"
#include "vast/config_options.hpp"
#include "vast/data.hpp"
#include "vast/detail/add_message_types.hpp"
#include "vast/detail/append.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/env.hpp"
#include "vast/detail/installdirs.hpp"
#include "vast/detail/load_contents.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/settings.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/type.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>
#include <caf/openssl/manager.hpp>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <optional>
#include <unordered_set>

namespace vast {

namespace {

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
to_config_key(std::string_view key, std::string_view prefix) {
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

caf::expected<caf::config_value> to_config_value(std::string_view value) {
  // Lists of strings can show up as `foo,bar,baz`.
  auto xs = detail::split(value, ",", "\\");
  if (xs.size() == 1)
    return caf::config_value::parse(value); // no list
  std::vector<caf::config_value> result;
  for (const auto& x : xs)
    if (auto cfg_val = to_config_value(x))
      result.push_back(*std::move(cfg_val));
    else
      return cfg_val.error();
  return caf::config_value{std::move(result)};
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
    // Support both tenzir.* and vast.* stems, as well as *.yaml and *.yml
    // extensions
    auto select
      = [&](std::string_view name) -> caf::expected<std::filesystem::path> {
      auto base_yaml = fmt::format("{}.yaml", name);
      auto base_yml = fmt::format("{}.yml", name);
      auto conf_yaml = dir / base_yaml;
      auto conf_yml = dir / base_yml;
      std::error_code err{};
      const auto exists_conf_yaml = std::filesystem::exists(conf_yaml, err);
      if (err)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("failed to check if {} file "
                                           "exists in {}: {}",
                                           base_yaml, dir, err.message()));
      const auto exists_conf_yml = std::filesystem::exists(conf_yml, err);
      if (err)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("failed to check if {} file "
                                           "exists in {}: {}",
                                           base_yml, dir, err.message()));
      // We cannot decide which one to pick if we have two, so bail out.
      if (exists_conf_yaml && exists_conf_yml)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("detected both '{}' and "
                                           "'{}' files in {}",
                                           base_yaml, base_yml, dir));
      if (exists_conf_yaml)
        return conf_yaml;
      else if (exists_conf_yml)
        return conf_yml;
      return ec::no_error;
    };
    auto tenzir_config = select("tenzir");
    if (tenzir_config) {
      result.emplace_back(std::move(*tenzir_config));
      continue;
    }
    if (tenzir_config.error() != ec::no_error)
      return tenzir_config.error();
    auto vast_config = select("vast");
    if (vast_config) {
      result.emplace_back(std::move(*vast_config));
      continue;
    }
    if (vast_config.error() != ec::no_error)
      return vast_config.error();
  }
  // Second, consider command line and environment overrides. But only check
  // the environment if we don't have a config on the command line.
  if (cli_configs.empty()) {
    if (auto file = detail::getenv("TENZIR_CONFIG"))
      cli_configs.emplace_back(*file);
    else if (auto file = detail::getenv("VAST_CONFIG"))
      cli_configs.emplace_back(*file);
  }
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
  // If a config file is specified multiple times, keep only the latest mention
  // of it. We do this the naive O(n^2) way because there usually aren't many
  // config files and this is never called in a hot loop, and we need this to be
  // order-preserving with removing the _earlier_ instances of duplicates.
  {
    auto unique_config_files = detail::stable_set<std::filesystem::path>{};
    std::for_each(std::make_move_iterator(config_files.rbegin()),
                  std::make_move_iterator(config_files.rend()),
                  [&](std::filesystem::path&& config_file) noexcept {
                    unique_config_files.insert(std::move(config_file));
                  });
    config_files.clear();
    std::copy(std::make_move_iterator(unique_config_files.rbegin()),
              std::make_move_iterator(unique_config_files.rend()),
              std::back_inserter(config_files));
  }
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
      // Rewrite the top-level key 'vast' to 'tenzir' for compatibility.
      auto tenzir_section
        = std::find_if(rec->begin(), rec->end(), [&](const auto& x) {
            return x.first == "tenzir";
          });
      auto vast_section
        = std::find_if(rec->begin(), rec->end(), [&](const auto& x) {
            return x.first == "vast";
          });
      if (vast_section != rec->end()) {
        fmt::print(stderr,
                   "In {}: The 'vast' config section name has been deprecated "
                   "in favor of 'tenzir'\n",
                   config);
      }
      if (vast_section != rec->end()) {
        if (tenzir_section != rec->end()) {
          return caf::make_error(
            ec::parse_error,
            fmt::format("failed to read config file {}: using 'tenzir' and "
                        "'vast' sections in the same file is not allowed.",
                        config));
        }
        vast_section->first = "tenzir";
      }
      merge(*rec, merged_config, policy::merge_lists::yes);
      loaded_config_files_singleton.push_back(config);
    }
  }
  return merged_config;
}

/// Merges VAST environment variables into a configuration.
caf::error merge_environment(record& config) {
  for (const auto& [key, value] : detail::environment()) {
    if (!value.empty()) {
      auto compat_to_config_key
        = [](const auto& key) -> std::optional<std::string> {
        if (auto result = to_config_key(key, "TENZIR"))
          return result;
        auto result = to_config_key(key, "VAST");
        if (result) {
          auto tenzir_key
            = fmt::format("TENZIR{}", std::string_view{key}.substr(4));
          if (detail::getenv(tenzir_key)) {
            fmt::print(stderr, "ignoring {} in favor of {}\n", key, tenzir_key);
            return {};
          }
        }
        return result;
      };
      if (auto config_key = compat_to_config_key(key)) {
        if (!config_key->starts_with("caf.")
            && !config_key->starts_with("plugins."))
          config_key->insert(0, "tenzir.");
        // These environment variables have been manually checked already.
        // Inserting them into the config would ignore higher-precedence values
        // from the command line.
        if (*config_key == "tenzir.bare-mode" || *config_key == "tenzir.config")
          continue;
        // Try first as vast::data, which is richer.
        if (auto x = to<data>(value)) {
          config[*config_key] = std::move(*x);
        } else if (auto config_value = to_config_value(value)) {
          if (auto x = to<data>(*config_value))
            config[*config_key] = std::move(*x);
          else
            return caf::make_error(
              ec::parse_error, fmt::format("could not convert environment "
                                           "variable {}={} to VAST value: {}",
                                           key, value, x.error()));
        } else {
          config[*config_key] = std::string{value};
        }
      }
    }
  }
  return caf::none;
}

caf::expected<caf::settings> to_settings(record config) {
  // Pre-process our configuration so that it can be properly parsed later.
  // Erase all null values because a caf::config_value has no such notion.
  for (auto i = config.begin(); i != config.end();) {
    if (caf::holds_alternative<caf::none_t>(i->second))
      i = config.erase(i);
    else
      ++i;
  }
  return to<caf::settings>(config);
}

} // namespace

std::vector<std::filesystem::path>
config_dirs(const caf::actor_system_config& config) {
  const auto bare_mode = caf::get_or(config.content, "tenzir.bare-mode", false);
  if (bare_mode)
    return {};
  auto result = std::vector<std::filesystem::path>{};
  if (auto xdg_config_home = detail::getenv("XDG_CONFIG_HOME")) {
    result.push_back(std::filesystem::path{*xdg_config_home} / "tenzir");
    result.push_back(std::filesystem::path{*xdg_config_home} / "vast");
  } else if (auto home = detail::getenv("HOME")) {
    result.push_back(std::filesystem::path{*home} / ".config" / "tenzir");
    result.push_back(std::filesystem::path{*home} / ".config" / "vast");
  }
  result.push_back(detail::install_configdir());
  return result;
}

const std::vector<std::filesystem::path>& loaded_config_files() {
  return loaded_config_files_singleton;
}

caf::expected<duration>
get_or_duration(const caf::settings& options, std::string_view key,
                duration fallback) {
  if (auto duration_arg = caf::get_if<std::string>(&options, key)) {
    if (auto duration = to<vast::duration>(*duration_arg))
      return *duration;
    return caf::make_error(ec::parse_error, fmt::format("cannot parse option "
                                                        "'{}' with value '{}' "
                                                        "as duration",
                                                        key, *duration_arg));
  }
  return caf::get_or(options, key, fallback);
}

configuration::configuration() {
  detail::add_message_types();
  // Load I/O module.
  load<caf::io::middleman>();
  // Initialize factories.
  factory<synopsis>::initialize();
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
  command_line.erase(caf_opt, command_line.end());
  // Do not use builtin config directories in "bare mode". We're checking this
  // here and putting directly into the actor_system_config because
  // the function config_dirs() relies on this already being there.
  if (auto it
      = std::find(command_line.begin(), command_line.end(), "--bare-mode");
      it != command_line.end()) {
    caf::put(content, "tenzir.bare-mode", true);
  } else if (auto vast_bare_mode = detail::getenv("TENZIR_BARE_MODE")) {
    if (*vast_bare_mode == "true") {
      caf::put(content, "tenzir.bare-mode", true);
    }
  } else if (auto vast_bare_mode = detail::getenv("VAST_BARE_MODE")) {
    if (*vast_bare_mode == "true") {
      caf::put(content, "tenzir.bare-mode", true);
    }
  }
  // Gather and parse all to-be-considered configuration files.
  std::vector<std::string> cli_configs;
  for (auto& arg : command_line)
    if (arg.starts_with("--config="))
      cli_configs.push_back(arg.substr(9));
  if (auto configs = collect_config_files(config_dirs(*this), cli_configs))
    config_files = std::move(*configs);
  else
    return configs.error();
  auto config = load_config_files(config_files);
  if (!config)
    return config.error();
  *config = flatten(*config);
  if (auto err = merge_environment(*config))
    return err;
  // From here on, we go into CAF land with the goal to put the configuration
  // into the members of this actor_system_config instance.
  auto settings = to_settings(std::move(*config));
  if (!settings)
    return settings.error();
  if (auto err = embed_config(*settings))
    return err;
  // Work around CAF quirk where options in the `openssl` group have no effect
  // if they are not seen by the native option or config file parsers.
  openssl_certificate = caf::get_or(content, "caf.openssl.certificate", "");
  openssl_key = caf::get_or(content, "caf.openssl.key", "");
  openssl_passphrase = caf::get_or(content, "caf.openssl.passphrase", "");
  openssl_capath = caf::get_or(content, "caf.openssl.capath", "");
  openssl_cafile = caf::get_or(content, "caf.openssl.cafile", "");
  // Detect when plugins, plugin-dirs, or schema-dirs are specified on the
  // command line. This needs to happen before the regular parsing of the
  // command line since plugins may add additional commands and schemas.
  auto is_not_plugin_opt = [](auto& x) {
    return !x.starts_with("--plugins=") && !x.starts_with("--plugin-dirs=")
           && !x.starts_with("--schema-dirs=");
  };
  auto plugin_opt = std::stable_partition(
    command_line.begin(), command_line.end(), is_not_plugin_opt);
  auto plugin_args = std::vector<std::string>{};
  // Because these options need to be parsed early we also need to handle their
  // environment equivalents early. We do so by going through the
  // caf::config_option_set parser such that they use the same syntax as
  // command-line options. We also get escaping for free this way.
  if (auto vast_plugin_dirs = detail::getenv("TENZIR_PLUGINS")) {
    plugin_args.push_back(fmt::format("--plugins={}", *vast_plugin_dirs));
  } else if (auto vast_plugin_dirs = detail::getenv("VAST_PLUGINS")) {
    plugin_args.push_back(fmt::format("--plugins={}", *vast_plugin_dirs));
  }
  if (auto vast_plugin_dirs = detail::getenv("TENZIR_PLUGIN_DIRS")) {
    plugin_args.push_back(fmt::format("--plugin-dirs={}", *vast_plugin_dirs));
  } else if (auto vast_plugin_dirs = detail::getenv("VAST_PLUGIN_DIRS")) {
    plugin_args.push_back(fmt::format("--plugin-dirs={}", *vast_plugin_dirs));
  }
  if (auto vast_schema_dirs = detail::getenv("TENZIR_SCHEMA_DIRS")) {
    plugin_args.push_back(fmt::format("--schema-dirs={}", *vast_schema_dirs));
  } else if (auto vast_schema_dirs = detail::getenv("VAST_SCHEMA_DIRS")) {
    plugin_args.push_back(fmt::format("--schema-dirs={}", *vast_schema_dirs));
  }
  // Copy over the specific plugin options.
  std::move(plugin_opt, command_line.end(), std::back_inserter(plugin_args));
  command_line.erase(plugin_opt, command_line.end());
  auto plugin_opts
    = config_options{}
        .add<std::vector<std::string>>("?tenzir", "schema-dirs", "")
        .add<std::vector<std::string>>("?tenzir", "plugin-dirs", "")
        .add<std::vector<std::string>>("?tenzir", "plugins", "");
  auto [ec, it] = plugin_opts.parse(content, plugin_args);
  if (ec != caf::pec::success) {
    VAST_ASSERT(it != plugin_args.end());
    return caf::make_error(ec, fmt::format("failed to parse option '{}'", *it));
  }
  VAST_ASSERT(it == plugin_args.end());
  // Now parse all CAF options from the command line. Prior to doing so, we
  // clear the config_file_path first so it does not use caf-application.ini as
  // fallback during actor_system_config::parse().
  config_file_path.clear();
  auto result = actor_system_config::parse(std::move(caf_args));
  // Load OpenSSL last because it uses the parsed configuration.
  const auto use_encryption
    = !openssl_certificate.empty() || !openssl_key.empty()
      || !openssl_passphrase.empty() || !openssl_capath.empty()
      || !openssl_cafile.empty();
  if (use_encryption)
    load<caf::openssl::manager>();
  return result;
}

caf::error configuration::embed_config(const caf::settings& settings) {
  for (auto& [key, value] : settings) {
    // The configuration must have been fully flattened because we cannot
    // mangle dictionaries in here.
    VAST_ASSERT(!caf::holds_alternative<caf::config_value::dictionary>(value));
    // The member custom_options_ (a config_option_set) is the only place that
    // contains the valid type information, as defined by the command
    // hierarchy. The passed in config (file and environment) must abide to it.
    if (const auto* option = custom_options_.qualified_name_lookup(key)) {
      auto val = value;
      if (auto err = option->sync(val))
        return err;
      else
        put(content, key, std::move(val));
    } else {
      // If the option is not relevant to CAF's custom options, we just store
      // the value directly in the content.
      put(content, key, value);
    }
  }
  return caf::none;
}

} // namespace vast
