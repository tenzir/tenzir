//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/print.hpp"
#include "tenzir/concept/printable/string/char.hpp"
#include "tenzir/concept/printable/string/string.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/operators.hpp"
#include "tenzir/detail/stable_set.hpp"
#include "tenzir/type.hpp"

#include <caf/expected.hpp>

#include <filesystem>
#include <string>
#include <vector>

namespace tenzir {

class data;

/// A sequence of types.
class module : detail::equality_comparable<module> {
public:
  using value_type = type;
  using const_iterator = std::vector<value_type>::const_iterator;
  using iterator = std::vector<value_type>::iterator;

  friend bool operator==(const module& x, const module& y);

  /// Merges two module.
  /// @param s1 The first module.
  /// @param s2 The second module.
  /// @returns The union of *s1* and *s2* if the inputs are disjunct.
  static caf::expected<module> merge(const module& s1, const module& s2);

  /// Combines two module, prefering definitions from s2 on conflicts.
  /// @param s1 The first module.
  /// @param s2 The second module.
  /// @returns The combination of *s1* and *s2*.
  static module combine(const module& s1, const module& s2);

  /// Adds a new type to the module.
  /// @param t The type to add.
  /// @returns `true` on success.
  bool add(value_type t);

  /// Retrieves the type for a given name.
  /// @param name The name of the type to lookup.
  /// @returns The type with name *name* or `nullptr if no such type exists.
  value_type* find(std::string_view name);

  //! @copydoc find(const std::string& name)
  [[nodiscard]] const value_type* find(std::string_view name) const;

  // -- container API ----------------------------------------------------------

  [[nodiscard]] const_iterator begin() const;
  [[nodiscard]] const_iterator end() const;
  [[nodiscard]] size_t size() const;
  [[nodiscard]] bool empty() const;
  void clear();

  // -- CAF -------------------------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, module& x) {
    return f.object(x)
      .pretty_name("tenzir.module")
      .fields(f.field("types", x.types_));
  }

private:
  std::vector<value_type> types_;
};

/// Loads the complete module for an invocation by combining the configured
/// modules with the ones passed directly as command line options.
/// @param options The set of command line options.
/// @returns The parsed module.
caf::expected<module> get_module(const caf::settings& options);

/// Gathers the list of paths to traverse for loading module or taxonomies data.
/// @param cfg The application config.
/// module directories.
/// @returns The list of module directories.
detail::stable_set<std::filesystem::path>
get_module_dirs(const caf::actor_system_config& cfg);

/// Loads a single module file.
/// @param module_file The file path.
/// @returns The parsed module.
caf::expected<module> load_module(const std::filesystem::path& module_file);

/// Loads module files from the given directories.
/// @param module_dirs The directories to load modules from.
/// @param max_recursion The maximum number of nested directories to traverse
/// before aborting.
/// @note Modules from the same directory are merged, but directories are
/// combined. It is designed so types that exist in later paths can override the
/// earlier ones, but the same mechanism makes no sense inside of a single
/// directory unless we specify a specific order of traversal.
caf::expected<tenzir::module>
load_module(const detail::stable_set<std::filesystem::path>& module_dirs,
            size_t max_recursion = defaults::max_recursion);

/// Loads modules according to the configuration. This is a convenience wrapper
/// around *get_module_dirs* and *load_module*.
caf::expected<tenzir::module> load_module(const caf::actor_system_config& cfg);

/// Loads taxonomies according to the configuration.
/// Mainly used for loading concepts into the global concept registry.
/// @param cfg The application config.
/// @returns The loaded taxonomies.
auto load_taxonomies(const caf::actor_system_config& cfg)
  -> caf::expected<taxonomies>;

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::module> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::module& value, FormatContext& ctx) const {
    auto out = ctx.out();
    for (const auto& t : value) {
      auto f = [&]<tenzir::concrete_type T>(const T& x) {
        out = fmt::format_to(out, "type {} = {}\n", t.name(), x);
      };
      match(t, f);
    }
    return out;
  }
};

} // namespace fmt
