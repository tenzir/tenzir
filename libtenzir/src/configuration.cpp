//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/configuration.hpp"

#include "tenzir/concept/convertible/to.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/config.hpp"
#include "tenzir/config_options.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/add_message_types.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/installdirs.hpp"
#include "tenzir/detail/load_contents.hpp"
#include "tenzir/detail/stable_set.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/factory.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_index_factory.hpp"

#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>
#include <caf/openssl/manager.hpp>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iterator>
#include <optional>

namespace tenzir {

namespace {

std::vector<config_file> loaded_config_files_singleton = {};

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
auto to_config_key(std::string_view key,
                   std::string_view prefix) -> std::optional<std::string> {
  TENZIR_ASSERT(!prefix.empty());
  // PREFIX_X is the shortest allowed key.
  if (prefix.size() + 2 > key.size()) {
    return std::nullopt;
  }
  if (!key.starts_with(prefix) || key[prefix.size()] != '_') {
    return std::nullopt;
  }
  auto suffix = key.substr(prefix.size() + 1);
  // From here on, "__" is the record separator and '_' translates into '-'.
  auto xs = detail::to_strings(detail::split(suffix, "__"));
  for (auto& x : xs) {
    for (auto& c : x) {
      c = (c == '_')
            ? '-'
            : static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
  }
  return detail::join(xs, ".");
}

auto to_config_value(std::string_view value)
  -> caf::expected<caf::config_value> {
  // Lists of strings can show up as `foo,bar,baz`.
  auto xs = detail::split_escaped(value, ",", "\\");
  if (xs.size() == 1) {
    return caf::config_value::parse(value); // no list
  }
  std::vector<caf::config_value> result;
  for (const auto& x : xs) {
    if (auto cfg_val = to_config_value(x)) {
      result.push_back(*std::move(cfg_val));
    } else {
      return cfg_val.error();
    }
  }
  return caf::config_value{std::move(result)};
}

auto check_yaml_file(const std::filesystem::path& dir,
                     std::string_view basename)
  -> caf::expected<std::optional<std::filesystem::path>> {
  auto err = std::error_code{};
  const auto path_yaml = dir / fmt::format("{}.yaml", basename);
  const auto yaml_exists = std::filesystem::exists(path_yaml, err);
  if (err) {
    return diagnostic::error("{}", err.message())
      .note("failed to check if `{}` exists", path_yaml)
      .to_error();
  }
  const auto path_yml = dir / fmt::format("{}.yml", basename);
  const auto yml_exists = std::filesystem::exists(path_yml, err);
  if (err) {
    return diagnostic::error("{}", err.message())
      .note("failed to check if `{}` exists", path_yaml)
      .to_error();
  }
  if (yaml_exists and yml_exists) {
    return diagnostic::error("both `{}` and `{}` exist", path_yaml, path_yml)
      .to_error();
  }
  if (yaml_exists) {
    return path_yaml;
  }
  if (yml_exists) {
    return path_yml;
  }
  return std::nullopt;
};

auto collect_config_files(const std::vector<std::filesystem::path>& dirs,
                          std::vector<std::string> cli_configs)
  -> caf::expected<std::vector<config_file>> {
  auto result = std::vector<config_file>{};
  // First, go through all config file directories and gather config files
  // there. We populate the member variable `config_files` instead of
  // `config_file_path` in the base class so that we can support multiple
  // configuration files.
  for (const auto& dir : dirs) {
    auto tenzir_config = check_yaml_file(dir, "tenzir");
    if (not tenzir_config) {
      return tenzir_config.error();
    }
    if (*tenzir_config) {
      result.push_back(config_file{
        .path = std::move(**tenzir_config),
        .plugin = {},
      });
    }
    // Iterate over the plugin subdirectory to check plugin-sepcific config
    // directories.
    auto err = std::error_code{};
    const auto plugin_dir = dir / "plugin";
    const auto plugin_dir_exists = std::filesystem::exists(plugin_dir, err);
    if (err) {
      return diagnostic::error("{}", err.message())
        .note("failed to check if `{}` exists", plugin_dir)
        .to_error();
    }
    if (not plugin_dir_exists) {
      continue;
    }
    auto plugin_dir_iter = std::filesystem::directory_iterator{plugin_dir, err};
    if (err) {
      return diagnostic::error("{}", err.message())
        .note("failed to list directory `{}`", plugin_dir)
        .to_error();
    }
    auto added_plugin_configs_per_dir = std::unordered_set<std::string>{};
    for (const auto& file : plugin_dir_iter) {
      const auto& path = file.path();
      const auto& plugin = path.stem().string();
      if (path.extension() != ".yaml" and path.extension() != ".yml") {
        continue;
      }
      const auto [_, inserted] = added_plugin_configs_per_dir.insert(plugin);
      if (not inserted) {
        return diagnostic::error("multiple config files for plugin `{}` in "
                                 "`{}`",
                                 plugin, plugin_dir)
          .to_error();
      }
      result.push_back(config_file{
        .path = path,
        .plugin = plugin,
      });
    }
  }
  // Second, consider command line and environment overrides. But only check
  // the environment if we don't have a config on the command line.
  if (cli_configs.empty()) {
    if (auto file = detail::getenv("TENZIR_CONFIG")) {
      cli_configs.emplace_back(*file);
    }
  }
  for (const auto& file : cli_configs) {
    result.push_back({
      .path = file,
      .plugin = {},
    });
  }
  return result;
}

auto load_config_files(std::vector<config_file> config_files)
  -> caf::expected<record> {
  // If a config file is specified multiple times, keep only the latest mention
  // of it. We do this the naive O(n^2) way because there usually aren't many
  // config files and this is never called in a hot loop, and we need this to be
  // order-preserving with removing the _earlier_ instances of duplicates.
  {
    auto unique_config_files = detail::stable_set<config_file>{};
    std::ranges::move(config_files, std::inserter(unique_config_files,
                                                  unique_config_files.end()));
    config_files.clear();
    std::ranges::move(unique_config_files, std::back_inserter(config_files));
  }
  TENZIR_ASSERT(loaded_config_files_singleton.empty(),
                "config files may be loaded at most once");
  // Parse and merge all configuration files.
  auto merged_config = record{};
  for (const auto& config : config_files) {
    auto err = std::error_code{};
    if (std::filesystem::exists(config.path, err)) {
      auto contents = detail::load_contents(config.path);
      if (not contents) {
        return caf::make_error(ec::parse_error,
                               fmt::format("failed to read config file {}: {}",
                                           config.path, contents.error()));
      }
      auto yaml = from_yaml(*contents);
      if (not yaml) {
        return caf::make_error(ec::parse_error,
                               fmt::format("failed to read config file {}: {}",
                                           config.path, yaml.error()));
      }
      // Skip empty config files.
      if (caf::holds_alternative<caf::none_t>(*yaml)) {
        continue;
      }
      auto* rec = caf::get_if<record>(&*yaml);
      if (not rec) {
        return caf::make_error(ec::parse_error,
                               fmt::format("failed to read config file {}: not "
                                           "a map of key-value pairs",
                                           config.path));
      }
      // If this is a plugin config, move the contents accordingly.
      if (config.plugin) {
        *yaml = record{
          {"plugins",
           record{
             {*config.plugin, std::move(*yaml)},
           }},
        };
      }
      merge(*rec, merged_config, policy::merge_lists::yes);
      loaded_config_files_singleton.push_back(config);
    }
  }
  return merged_config;
}

/// Merges Tenzir environment variables into a configuration.
auto merge_environment(record& config) -> caf::error {
  for (const auto& [key, value] : detail::environment()) {
    if (!value.empty()) {
      if (auto config_key = to_config_key(key, "TENZIR")) {
        if (!config_key->starts_with("caf.")
            && !config_key->starts_with("plugins.")) {
          config_key->insert(0, "tenzir.");
        }
        // These environment variables have been manually checked already.
        // Inserting them into the config would ignore higher-precedence values
        // from the command line.
        if (*config_key == "tenzir.bare-mode"
            || *config_key == "tenzir.config") {
          continue;
        }
        // Try first as tenzir::data, which is richer.
        if (auto x = to<data>(value)) {
          config[*config_key] = std::move(*x);
        } else if (auto config_value = to_config_value(value)) {
          if (auto x = to<data>(*config_value)) {
            config[*config_key] = std::move(*x);
          } else {
            return caf::make_error(
              ec::parse_error, fmt::format("could not convert environment "
                                           "variable {}={} to Tenzir value: {}",
                                           key, value, x.error()));
          }
        } else {
          config[*config_key] = std::string{value};
        }
      }
    }
  }
  return caf::none;
}

auto to_settings(record config) -> caf::expected<caf::settings> {
  // Pre-process our configuration so that it can be properly parsed later.
  // Erase all null values because a caf::config_value has no such notion.
  for (auto i = config.begin(); i != config.end();) {
    if (caf::holds_alternative<caf::none_t>(i->second)) {
      i = config.erase(i);
    } else {
      ++i;
    }
  }
  return to<caf::settings>(config);
}

auto config_dirs(bool bare_mode) -> std::vector<std::filesystem::path> {
  if (bare_mode) {
    return {};
  }
  auto result = std::vector<std::filesystem::path>{};
  if (auto xdg_config_home = detail::getenv("XDG_CONFIG_HOME")) {
    result.push_back(std::filesystem::path{*xdg_config_home} / "tenzir");
  } else if (auto home = detail::getenv("HOME")) {
    result.push_back(std::filesystem::path{*home} / ".config" / "tenzir");
  }
  result.push_back(detail::install_configdir());
  return result;
}

} // namespace

auto config_dirs(const caf::actor_system_config& cfg)
  -> std::vector<std::filesystem::path> {
  return config_dirs(caf::get_or(cfg.content, "tenzir.bare-mode", false));
}

auto config_dirs(const record& cfg) -> std::vector<std::filesystem::path> {
  auto fallback = false;
  return config_dirs(get_or(cfg, "tenzir.bare-mode", fallback));
}

auto loaded_config_files() -> const std::vector<config_file>& {
  return loaded_config_files_singleton;
}

auto get_or_duration(const caf::settings& options, std::string_view key,
                     duration fallback) -> caf::expected<duration> {
  if (const auto* duration_arg = caf::get_if<std::string>(&options, key)) {
    if (auto duration = to<tenzir::duration>(*duration_arg)) {
      return *duration;
    }
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
  auto register_extension_types
    = []<concrete_type... Ts>(caf::detail::type_list<Ts...>) {
        (static_cast<void>(Ts::arrow_type::register_extension()), ...);
      };
  register_extension_types(
    caf::detail::tl_filter_t<concrete_types, has_extension_type>{});
}

auto configuration::parse(int argc, char** argv) -> caf::error {
  // The main objective of this function is to parse the command line and put
  // it into the actor_system_config instance (`content`), which components
  // throughout Tenzir query to find out the application settings. This process
  // has several sequencing intricacies because it also loads configuration
  // files and considers environment variables while parsing the command line.
  //
  // A major issue is that we have to use caf::settings and cannot use
  // tenzir::record as unified representation, exacerbating the complexity of
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
  TENZIR_ASSERT(argc > 0);
  TENZIR_ASSERT(argv != nullptr);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
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
  for (auto& option : command_line) {
    for (const auto& [old, new_] : replacements) {
      if (option == old) {
        option = new_;
      }
    }
  }
  // Remove CAF options from the command line; we'll parse them at the very
  // end.
  auto is_tenzir_opt = [](const auto& x) {
    return !x.starts_with("--caf.");
  };
  auto caf_opt = std::stable_partition(command_line.begin(), command_line.end(),
                                       is_tenzir_opt);
  std::vector<std::string> caf_args;
  std::move(caf_opt, command_line.end(), std::back_inserter(caf_args));
  command_line.erase(caf_opt, command_line.end());
  // Do not use builtin config directories in "bare mode". We're checking this
  // here and putting directly into the actor_system_config because
  // the function config_dirs() relies on this already being there.
  auto falsy_env_values = std::set<std::string_view>{"", "0", "false", "FALSE"};
  if (auto it
      = std::find(command_line.begin(), command_line.end(), "--bare-mode");
      it != command_line.end()) {
    caf::put(content, "tenzir.bare-mode", true);
  } else if (auto tenzir_bare_mode = detail::getenv("TENZIR_BARE_MODE")) {
    caf::put(content, "tenzir.bare-mode",
             not falsy_env_values.contains(*tenzir_bare_mode));
  }
  // Gather and parse all to-be-considered configuration files.
  std::vector<std::string> cli_configs;
  for (auto& arg : command_line) {
    if (arg.starts_with("--config=")) {
      cli_configs.push_back(arg.substr(9));
    }
  }
  if (auto configs = collect_config_files(config_dirs(*this), cli_configs)) {
    config_files = std::move(*configs);
  } else {
    return configs.error();
  }
  auto config = load_config_files(config_files);
  if (!config) {
    return config.error();
  }
  *config = flatten(*config);
  if (auto err = merge_environment(*config)) {
    return err;
  }
  // Fallback handling for system default paths that may be provided by the
  // runtime system and should win over the build-time defaults but loose if set
  // via any other method.
  if (!config->contains("tenzir.state-directory")) {
    // Provided by systemd when StateDirectory= is set in the unit.
    if (auto state_directory = detail::getenv("STATE_DIRECTORY")) {
      (*config)["tenzir.state-directory"] = std::string{*state_directory};
    }
  }
  if (!config->contains("tenzir.log-directory")) {
    // Provided by systemd when LogsDirectory= is set in the unit.
    if (auto log_directory = detail::getenv("LOGS_DIRECTORY")) {
      (*config)["tenzir.log-file"]
        = fmt::format("{}/server.log", *log_directory);
    }
  }
  if (!config->contains("tenzir.cache-directory")) {
    // Provided by systemd when CacheDirectory= is set in the unit.
    if (auto cache_directory = detail::getenv("CACHE_DIRECTORY")) {
      (*config)["tenzir.cache-directory"] = std::string{*cache_directory};
    }
  }
  // Set some defaults that can only be derived at runtime.
  if (!config->contains("tenzir.cache-directory")) {
    auto env_path_writable
      = [&](std::string_view key,
            auto... suffix) -> std::optional<std::filesystem::path> {
      auto x = detail::getenv(key);
      if (!x) {
        return std::nullopt;
      }
      auto path = (std::filesystem::path{*x} / ... / suffix);
      std::error_code ec;
      if (std::filesystem::exists(path, ec)) {
        if (std::filesystem::is_directory(path, ec)) {
          if (::access(path.string().c_str(), R_OK | W_OK | X_OK) == 0) {
            return path;
          }
          return std::nullopt;
        }
        return std::nullopt;
      }
      // Try to create.
      ec = {};
      std::filesystem::create_directory(path, ec);
      if (!ec) {
        return path;
      }
      return std::nullopt;
    };
#ifdef TENZIR_MACOS
#  define HOME_CACHE_PATH "Library", "Caches", "tenzir"
#else
#  define HOME_CACHE_PATH ".cache", "tenzir"
#endif
    auto& value = (*config)["tenzir.cache-directory"];
    if (auto x = env_path_writable("XDG_CACHE_HOME", "tenzir")) {
      value = x->string();
    } else if (auto x = env_path_writable("XDG_HOME_DIR", HOME_CACHE_PATH)) {
      value = x->string();
    } else if (auto x = env_path_writable("HOME", HOME_CACHE_PATH)) {
      value = x->string();
    } else {
      auto ec = std::error_code{};
      auto tmp = std::filesystem::temp_directory_path(ec);
      if (ec) {
        return caf::make_error(ec::filesystem_error,
                               "failed to determine temp_directory_path");
      }
      auto path = tmp / "tenzir" / "cache" / fmt::format("{:}", getuid());
      std::filesystem::create_directories(path, ec);
      if (ec) {
        return caf::make_error(
          ec::filesystem_error,
          fmt::format("failed to create cache directory {}: {}", path, ec));
      }
      value = path.string();
    }
  }
  // From here on, we go into CAF land with the goal to put the configuration
  // into the members of this actor_system_config instance.
  auto settings = to_settings(std::move(*config));
  if (!settings) {
    return settings.error();
  }
  if (auto err = embed_config(*settings)) {
    return err;
  }
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
    return !x.starts_with("--plugins=") && !x.starts_with("--disable-plugins=")
           && !x.starts_with("--plugin-dirs=")
           && !x.starts_with("--schema-dirs=")
           && !x.starts_with("--package-dirs=");
  };
  auto plugin_opt = std::stable_partition(
    command_line.begin(), command_line.end(), is_not_plugin_opt);
  auto plugin_args = std::vector<std::string>{};
  // Because these options need to be parsed early we also need to handle their
  // environment equivalents early. We do so by going through the
  // caf::config_option_set parser such that they use the same syntax as
  // command-line options. We also get escaping for free this way.
  if (auto tenzir_plugins = detail::getenv("TENZIR_PLUGINS")) {
    plugin_args.push_back(fmt::format("--plugins={}", *tenzir_plugins));
  }
  if (auto tenzir_disable_plugins = detail::getenv("TENZIR_DISABLE_PLUGINS")) {
    plugin_args.push_back(
      fmt::format("--disable-plugins={}", *tenzir_disable_plugins));
  }
  if (auto tenzir_plugin_dirs = detail::getenv("TENZIR_PLUGIN_DIRS")) {
    plugin_args.push_back(fmt::format("--plugin-dirs={}", *tenzir_plugin_dirs));
  }
  if (auto tenzir_schema_dirs = detail::getenv("TENZIR_SCHEMA_DIRS")) {
    plugin_args.push_back(fmt::format("--schema-dirs={}", *tenzir_schema_dirs));
  }
  // Package dirs don't need to be parsed early, but we still want to have
  // the same command-line parsing logic so that a single string is
  // automatically promoted to a list.
  if (auto tenzir_package_dirs = detail::getenv("TENZIR_PACKAGE_DIRS")) {
    plugin_args.push_back(
      fmt::format("--package-dirs={}", *tenzir_package_dirs));
  }
  // Copy over the specific plugin options.
  std::move(plugin_opt, command_line.end(), std::back_inserter(plugin_args));
  command_line.erase(plugin_opt, command_line.end());
  auto plugin_opts
    = config_options{}
        .add<std::vector<std::string>>("?tenzir", "schema-dirs", "")
        .add<std::vector<std::string>>("?tenzir", "plugin-dirs", "")
        .add<std::vector<std::string>>("?tenzir", "package-dirs", "")
        .add<std::vector<std::string>>("?tenzir", "plugins", "")
        .add<std::vector<std::string>>("?tenzir", "disable-plugins", "");
  auto [ec, it] = plugin_opts.parse(content, plugin_args);
  if (ec != caf::pec::success) {
    TENZIR_ASSERT(it != plugin_args.end());
    return caf::make_error(ec, fmt::format("failed to parse option '{}'", *it));
  }
  TENZIR_ASSERT(it == plugin_args.end());
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
  if (use_encryption) {
    load<caf::openssl::manager>();
  }
  return result;
}

auto configuration::embed_config(const caf::settings& settings) -> caf::error {
  for (const auto& [key, value] : settings) {
    // The configuration must have been fully flattened because we cannot
    // mangle dictionaries in here.
    TENZIR_ASSERT(
      !caf::holds_alternative<caf::config_value::dictionary>(value));
    // The member custom_options_ (a config_option_set) is the only place that
    // contains the valid type information, as defined by the command
    // hierarchy. The passed in config (file and environment) must abide to it.
    if (const auto* option = custom_options_.qualified_name_lookup(key)) {
      auto val = value;
      if (auto err = option->sync(val)) {
        return err;
      }
      put(content, key, std::move(val));
    } else {
      // If the option is not relevant to CAF's custom options, we just store
      // the value directly in the content.
      put(content, key, value);
    }
  }
  return caf::none;
}

} // namespace tenzir
