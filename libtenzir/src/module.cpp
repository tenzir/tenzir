//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/module.hpp"

#include "tenzir/concept/convertible/data.hpp"
#include "tenzir/concept/parseable/parse.hpp"
#include "tenzir/concept/parseable/tenzir/schema.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/filter_dir.hpp"
#include "tenzir/detail/installdirs.hpp"
#include "tenzir/detail/load_contents.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/taxonomies.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/deserializer.hpp>

#include <filesystem>

namespace tenzir {

caf::expected<module> module::merge(const module& s1, const module& s2) {
  auto result = s2;
  for (const auto& t : s1) {
    if (const auto* u = s2.find(t.name())) {
      if (t != *u && t.name() == u->name())
        // Type clash: cannot accommodate two types with same name.
        return caf::make_error(ec::format_error,
                               fmt::format("type clash: cannot accommodate two "
                                           "types with the same name: {}",
                                           t.name()));
    } else {
      result.types_.push_back(t);
    }
  }
  return result;
}

module module::combine(const module& s1, const module& s2) {
  auto result = s1;
  for (const auto& t : s2) {
    if (auto* x = result.find(t.name()))
      *x = t;
    else
      result.add(t);
  }
  return result;
}

bool module::add(module::value_type t) {
  if (find(t.name()) != nullptr)
    return false;
  types_.push_back(std::move(t));
  return true;
}

module::value_type* module::find(std::string_view name) {
  for (auto& t : types_)
    if (t.name() == name)
      return &t;
  return nullptr;
}

const module::value_type* module::find(std::string_view name) const {
  for (const auto& t : types_)
    if (t.name() == name)
      return &t;
  return nullptr;
}

module::const_iterator module::begin() const {
  return types_.begin();
}

module::const_iterator module::end() const {
  return types_.end();
}

size_t module::size() const {
  return types_.size();
}

bool module::empty() const {
  return types_.empty();
}

void module::clear() {
  types_.clear();
}

bool operator==(const module& x, const module& y) {
  return x.types_ == y.types_;
}

caf::expected<module> get_module(const caf::settings& options) {
  // Get the default module from the registry.
  TENZIR_DIAGNOSTIC_PUSH
  TENZIR_DIAGNOSTIC_IGNORE_DEPRECATED
  const auto* module_reg_ptr = modules::global_module();
  TENZIR_DIAGNOSTIC_POP
  auto module = module_reg_ptr ? *module_reg_ptr : tenzir::module{};
  // Update with an alternate module, if requested.
  auto sc = caf::get_if<std::string>(&options, "tenzir.import.schema");
  auto mf = caf::get_if<std::string>(&options, "tenzir.import.schema-file");
  if (sc && mf)
    return caf::make_error(ec::invalid_configuration,
                           "had both schema and schema-file "
                           "provided");
  if (!sc && !mf)
    return module;
  caf::expected<tenzir::module> update = caf::error{};
  if (sc)
    update = to<tenzir::module>(*sc);
  else
    update = load_module(*mf);
  if (!update)
    return update.error();
  return module::combine(module, *update);
}

detail::stable_set<std::filesystem::path>
get_module_dirs(const caf::actor_system_config& cfg) {
  const auto bare_mode = caf::get_or(cfg, "tenzir.bare-mode", false);
  detail::stable_set<std::filesystem::path> result;
  const auto datadir = detail::install_datadir();
  result.insert(datadir / "schema");
  for (const auto& plugin : plugins::get()) {
    auto dir = datadir / "plugin" / plugin->name() / "schema";
    auto err = std::error_code{};
    if (std::filesystem::exists(dir, err))
      result.insert(std::move(dir));
  }
  if (!bare_mode) {
    result.insert(detail::install_configdir() / "schema");
    if (auto xdg_config_home = detail::getenv("XDG_CONFIG_HOME")) {
      result.insert(std::filesystem::path{*xdg_config_home} / "tenzir"
                    / "schema");
    } else if (auto home = detail::getenv("HOME")) {
      result.insert(std::filesystem::path{*home} / ".config" / "tenzir"
                    / "schema");
    }
  }
  if (auto dirs = detail::unpack_config_list_to_vector<std::string>( //
        cfg, "tenzir.schema-dirs"))
    result.insert(dirs->begin(), dirs->end());
  return result;
}

caf::expected<module> load_module(const std::filesystem::path& module_file) {
  if (module_file.empty())
    return caf::make_error(ec::filesystem_error, "empty path");
  auto str = detail::load_contents(module_file);
  if (!str)
    return str.error();
  return to<module>(*str);
}

caf::error
load_symbols(const std::filesystem::path& module_file, symbol_map& local) {
  if (module_file.empty())
    return caf::make_error(ec::filesystem_error, "empty path");
  auto str = detail::load_contents(module_file);
  if (!str)
    return str.error();
  auto p = symbol_map_parser{};
  if (!p(*str, local))
    return caf::make_error(ec::parse_error, "failed to load symbols from",
                           module_file.string());
  return caf::none;
}

caf::expected<module>
load_module(const detail::stable_set<std::filesystem::path>& module_dirs,
            size_t max_recursion) {
  if (max_recursion == 0)
    return ec::recursion_limit_reached;
  tenzir::module types;
  symbol_map global_symbols;
  for (const auto& dir : module_dirs) {
    TENZIR_VERBOSE("loading schemas from {}", dir);
    std::error_code err{};
    if (!std::filesystem::exists(dir, err)) {
      TENZIR_DEBUG("{} skips non-existing directory: {}", __func__, dir);
      continue;
    }
    auto filter = [](const std::filesystem::path& f) {
      return f.extension() == ".schema";
    };
    auto module_files
      = detail::filter_dir(dir, std::move(filter), max_recursion);
    if (!module_files)
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to filter schema dir at {}: "
                                         "{}",
                                         dir, module_files.error()));
    symbol_map local_symbols;
    for (const auto& f : *module_files) {
      TENZIR_DEBUG("loading schema {}", f);
      if (auto err = load_symbols(f, local_symbols))
        return err;
    }
    auto r = symbol_resolver{global_symbols, local_symbols};
    auto directory_module = r.resolve();
    if (!directory_module)
      return caf::make_error(ec::format_error, "failed to resolve types in",
                             dir.string(), directory_module.error().context());
    local_symbols.merge(std::move(global_symbols));
    global_symbols = std::move(local_symbols);
    types = module::combine(types, *directory_module);
  }
  return types;
}

caf::expected<tenzir::module> load_module(const caf::actor_system_config& cfg) {
  return load_module(get_module_dirs(cfg));
}

auto load_taxonomies(const caf::actor_system_config& cfg)
  -> caf::expected<taxonomies> {
  std::error_code err{};
  auto dirs = get_module_dirs(cfg);
  concepts_map concepts;
  for (const auto& dir : dirs) {
    TENZIR_DEBUG("loading taxonomies from {}", dir);
    const auto dir_exists = std::filesystem::exists(dir, err);
    if (err)
      TENZIR_WARN("failed to open directory {}: {}", dir, err.message());
    if (!dir_exists)
      continue;
    auto yamls = load_yaml_dir(dir);
    if (!yamls)
      return yamls.error();
    for (auto& [file, yaml] : *yamls) {
      TENZIR_DEBUG("extracting taxonomies from {}", file.string());
      if (auto err = convert(yaml, concepts, concepts_data_schema))
        return caf::make_error(ec::parse_error,
                               "failed to extract concepts from file",
                               file.string(), err.context());
    }
  }
  return tenzir::taxonomies{std::move(concepts)};
}

} // namespace tenzir
