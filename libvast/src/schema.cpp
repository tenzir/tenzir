//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/schema.hpp"

#include "vast/concept/parseable/parse.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/data.hpp"
#include "vast/detail/env.hpp"
#include "vast/detail/filter_dir.hpp"
#include "vast/detail/installdirs.hpp"
#include "vast/detail/load_contents.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/event_types.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"

#include <caf/actor_system_config.hpp>

#include <filesystem>

namespace vast {

caf::expected<schema> schema::merge(const schema& s1, const schema& s2) {
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

schema schema::combine(const schema& s1, const schema& s2) {
  auto result = s1;
  for (const auto& t : s2) {
    if (auto* x = result.find(t.name()))
      *x = t;
    else
      result.add(t);
  }
  return result;
}

bool schema::add(schema::value_type t) {
  if (caf::holds_alternative<none_type>(t) || t.name().empty()
      || find(t.name()) != nullptr)
    return false;
  types_.push_back(std::move(t));
  return true;
}

schema::value_type* schema::find(std::string_view name) {
  for (auto& t : types_)
    if (t.name() == name)
      return &t;
  return nullptr;
}

const schema::value_type* schema::find(std::string_view name) const {
  for (const auto& t : types_)
    if (t.name() == name)
      return &t;
  return nullptr;
}

schema::const_iterator schema::begin() const {
  return types_.begin();
}

schema::const_iterator schema::end() const {
  return types_.end();
}

size_t schema::size() const {
  return types_.size();
}

bool schema::empty() const {
  return types_.empty();
}

void schema::clear() {
  types_.clear();
}

bool operator==(const schema& x, const schema& y) {
  return x.types_ == y.types_;
}

caf::expected<schema> get_schema(const caf::settings& options) {
  // Get the default schema from the registry.
  const auto* schema_reg_ptr = event_types::get();
  auto schema = schema_reg_ptr ? *schema_reg_ptr : vast::schema{};
  // Update with an alternate schema, if requested.
  auto sc = caf::get_if<std::string>(&options, "vast.import.schema");
  auto sf = caf::get_if<std::string>(&options, "vast.import.schema-file");
  if (sc && sf)
    caf::make_error(ec::invalid_configuration,
                    "had both schema and schema-file "
                    "provided");
  if (!sc && !sf)
    return schema;
  caf::expected<vast::schema> update = caf::no_error;
  if (sc)
    update = to<vast::schema>(*sc);
  else
    update = load_schema(*sf);
  if (!update)
    return update.error();
  return schema::combine(schema, *update);
}

detail::stable_set<std::filesystem::path>
get_schema_dirs(const caf::actor_system_config& cfg) {
  const auto bare_mode = caf::get_or(cfg, "vast.bare-mode", false);
  detail::stable_set<std::filesystem::path> result;
  if (auto vast_schema_directories = detail::locked_getenv("VAST_SCHEMA_DIRS"))
    for (auto&& path : detail::split(*vast_schema_directories, ":"))
      result.insert({path});
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
    if (auto xdg_config_home = detail::locked_getenv("XDG_CONFIG_HOME"))
      result.insert(std::filesystem::path{*xdg_config_home} / "vast"
                    / "schema");
    else if (auto home = detail::locked_getenv("HOME"))
      result.insert(std::filesystem::path{*home} / ".config" / "vast"
                    / "schema");
    if (auto dirs = caf::get_if<std::vector<std::string>>( //
          &cfg, "vast.schema-dirs"))
      result.insert(dirs->begin(), dirs->end());
  }
  return result;
}

caf::expected<schema> load_schema(const std::filesystem::path& schema_file) {
  if (schema_file.empty())
    return caf::make_error(ec::filesystem_error, "empty path");
  auto str = detail::load_contents(schema_file);
  if (!str)
    return str.error();
  return to<schema>(*str);
}

caf::error
load_symbols(const std::filesystem::path& schema_file, symbol_map& local) {
  if (schema_file.empty())
    return caf::make_error(ec::filesystem_error, "empty path");
  auto str = detail::load_contents(schema_file);
  if (!str)
    return str.error();
  auto p = symbol_map_parser{};
  if (!p(*str, local))
    return caf::make_error(ec::parse_error, "failed to load symbols from",
                           schema_file.string());
  return caf::none;
}

caf::expected<schema>
load_schema(const detail::stable_set<std::filesystem::path>& schema_dirs,
            size_t max_recursion) {
  if (max_recursion == 0)
    return ec::recursion_limit_reached;
  vast::schema types;
  symbol_map global_symbols;
  for (const auto& dir : schema_dirs) {
    VAST_VERBOSE("loading schemas from {}", dir);
    std::error_code err{};
    if (!std::filesystem::exists(dir, err)) {
      VAST_DEBUG("{} skips non-existing directory: {}", __func__, dir);
      continue;
    }
    auto filter = [](const std::filesystem::path& f) {
      return f.extension() == ".schema";
    };
    auto schema_files
      = detail::filter_dir(dir, std::move(filter), max_recursion);
    if (!schema_files)
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to filter schema dir at {}: "
                                         "{}",
                                         dir, schema_files.error()));
    symbol_map local_symbols;
    for (const auto& f : *schema_files) {
      VAST_DEBUG("loading schema {}", f);
      if (auto err = load_symbols(f, local_symbols))
        return err;
    }
    auto r = symbol_resolver{global_symbols, local_symbols};
    auto directory_schema = r.resolve();
    if (!directory_schema)
      return caf::make_error(ec::format_error, "failed to resolve types in",
                             dir.string(), directory_schema.error().context());
    local_symbols.merge(std::move(global_symbols));
    global_symbols = std::move(local_symbols);
    types = schema::combine(types, *directory_schema);
  }
  return types;
}

caf::expected<vast::schema> load_schema(const caf::actor_system_config& cfg) {
  return load_schema(get_schema_dirs(cfg));
}

} // namespace vast
