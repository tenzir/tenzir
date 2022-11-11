//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/configuration.hpp"

#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/detail/add_message_types.hpp"
#include "vast/detail/append.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/env.hpp"
#include "vast/detail/installdirs.hpp"
#include "vast/detail/load_contents.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/stable_set.hpp"
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
to_config_key(std::string_view key, std::string_view prefix = "VAST") {
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

caf::expected<caf::config_value>
to_config_value(const caf::config_value& value, const caf::config_option& opt) {
  // The config_option (from our commands) includes type information on what
  // value to accept. Command line values are checked against this.
  // Unfortunately our YAML config operates purely based on values. This means
  // we may have previously parsed a value incorrectly, e.g., where an atom was
  // expected we got a string. This trouble will be gone with CAF > 0.18, but
  // until then we have to *go back* into a string reprsentation to create
  // ambiguity (i.e., allow for parsing input as either string or atom), and
  // then come back to the config_value that matches the typing.
  auto no_quote_stringify = detail::overload{
    [](const auto& x) {
      return caf::deep_to_string(x);
    },
    [](const std::string& x) {
      return x;
    },
  };
  auto str = caf::visit(no_quote_stringify, value);
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
      return caf::make_error(ec::type_clash,
                             fmt::format( //
                               "failed to parse config option {} as {}: {}",
                               caf::deep_to_string(opt.full_name()),
                               caf::deep_to_string(opt.type_name()), str));
  }
  return result;
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
      merge(*rec, merged_config, policy::merge_lists::yes);
      loaded_config_files_singleton.push_back(config);
    }
  }
  return merged_config;
}

/// Merges VAST environment variables into a configuration.
caf::error merge_environment(record& config) {
  for (const auto& [key, value] : detail::environment())
    if (!value.empty())
      if (auto config_key = to_config_key(key)) {
        if (!config_key->starts_with("caf.")
            && !config_key->starts_with("plugins."))
          config_key->insert(0, "vast.");
        // These environment variables have been manually checked already.
        // Inserting them into the config would ignore higher-precedence values
        // from the command line.
        if (*config_key == "vast.bare-mode" || *config_key == "vast.config")
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
  return caf::none;
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

auto generate_default_value_for_argument_type(std::string_view type_name) {
  if (type_name.starts_with("uint") || type_name.starts_with("int")
      || type_name.starts_with("long")) {
    return "0";
  } else if (type_name == "timespan") {
    return "0s";
  } else if (type_name.starts_with("list")) {
    return "[]";
  }
  VAST_ASSERT(false && "option has type with no default value support");
  return "";
}

void sanitize_long_form_argument(std::string& argument,
                                 const vast::command& cmd) {
  auto dummy_options = caf::settings{};
  auto [state, _] = cmd.options.parse(dummy_options, {argument});
  if (state == caf::pec::not_an_option) {
    for (const auto& child_cmd : cmd.children) {
      sanitize_long_form_argument(argument, *child_cmd);
    }
  } else if (state == caf::pec::missing_argument) {
    auto name = argument.substr(2, argument.length() - 3);
    auto option = cmd.options.cli_long_name_lookup(name);
    if (!option) {
      // something is wrong with the long name options:
      // reveal this during the actual parsing.
      return;
    }
    auto option_type = option->type_name();
    auto options_type_default_val
      = generate_default_value_for_argument_type(option_type.data());
    argument.append(options_type_default_val);
  }
}

} // namespace

std::vector<std::filesystem::path>
config_dirs(const caf::actor_system_config& config) {
  const auto bare_mode = caf::get_or(config.content, "vast.bare-mode", false);
  if (bare_mode)
    return {};
  auto result = std::vector<std::filesystem::path>{};
  if (auto xdg_config_home = detail::getenv("XDG_CONFIG_HOME"))
    result.push_back(std::filesystem::path{*xdg_config_home} / "vast");
  else if (auto home = detail::getenv("HOME"))
    result.push_back(std::filesystem::path{*home} / ".config" / "vast");
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

caf::error configuration::parse(int argc, char** argv, const command& root) {
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
  for (auto& argument : command_line) {
    if (argument.starts_with("--")) {
      sanitize_long_form_argument(argument, root);
    }
  }
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
  // Do not use builtin config directories in "bare mode". We're checking this
  // here and putting directly into the actor_system_config because
  // the function config_dirs() relies on this already being there.
  if (auto it
      = std::find(command_line.begin(), command_line.end(), "--bare-mode");
      it != command_line.end())
    caf::put(content, "vast.bare-mode", true);
  else if (auto vast_bare_mode = detail::getenv("VAST_BARE_MODE"))
    if (*vast_bare_mode == "true")
      caf::put(content, "vast.bare-mode", true);
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
  if (auto vast_plugin_dirs = detail::getenv("VAST_PLUGINS"))
    plugin_args.push_back(fmt::format("--plugins={}", *vast_plugin_dirs));
  if (auto vast_plugin_dirs = detail::getenv("VAST_PLUGIN_DIRS"))
    plugin_args.push_back(fmt::format("--plugin-dirs={}", *vast_plugin_dirs));
  if (auto vast_schema_dirs = detail::getenv("VAST_SCHEMA_DIRS"))
    plugin_args.push_back(fmt::format("--schema-dirs={}", *vast_schema_dirs));
  // Newly added plugin arguments from environment variables
  // may contain empty values - sanitize them.
  for (auto& plugin_arg : plugin_args) {
    sanitize_long_form_argument(plugin_arg, root);
  }
  // Copy over the specific plugin options.
  std::move(plugin_opt, command_line.end(), std::back_inserter(plugin_args));
  command_line.erase(plugin_opt, command_line.end());
  auto plugin_opts
    = caf::config_option_set{}
        .add<std::vector<std::string>>("?vast", "schema-dirs", "")
        .add<std::vector<std::string>>("?vast", "plugin-dirs", "")
        .add<std::vector<std::string>>("?vast", "plugins", "");
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
  for (auto& [key, value] : settings) {
    // The configuration must have been fully flattened because we cannot
    // mangle dictionaries in here.
    VAST_ASSERT(!caf::holds_alternative<caf::config_value::dictionary>(value));
    // The member custom_options_ (a config_option_set) is the only place that
    // contains the valid type information, as defined by the command
    // hierarchy. The passed in config (file and environment) must abide to it.
    if (const auto* option = custom_options_.qualified_name_lookup(key)) {
      if (auto x = to_config_value(value, *option))
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
