//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/type_list.hpp"
#include "tenzir/location.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <caf/detail/is_one_of.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/detail/type_traits.hpp>

#include <functional>

namespace tenzir {

using argument_parser_data_types
  = detail::tl_map_t<caf::detail::tl_filter_not_type_t<data::types, pattern>,
                     as_located>;

using argument_parser_full_types = detail::tl_concat_t<
  argument_parser_data_types,
  detail::type_list<located<pipeline>, ast::expression, ast::simple_selector>>;

using argument_parser_bare_types
  = detail::tl_map_t<detail::tl_filter_t<argument_parser_full_types, is_located>,
                     caf::detail::value_type_of>;

using argument_parser_types
  = detail::tl_concat_t<argument_parser_full_types, argument_parser_bare_types>;

template <class T>
concept argument_parser_full_type
  = detail::tl_contains<argument_parser_full_types, T>::value;

template <class T>
concept argument_parser_bare_type
  = detail::tl_contains<argument_parser_bare_types, T>::value;

template <class T>
concept argument_parser_type
  = argument_parser_full_type<T> || argument_parser_bare_type<T>;

class argument_parser2 {
public:
  static auto operator_(std::string name) -> argument_parser2 {
    return argument_parser2{kind::op, std::move(name)};
  }

  static auto function(std::string name) -> argument_parser2 {
    return argument_parser2{kind::fn, std::move(name)};
  }

  // ------------------------------------------------------------------------

  /// Adds a required positional argument.
  template <argument_parser_type T>
  auto add(T& x, std::string meta) -> argument_parser2&;

  /// Adds an optional positional argument.
  template <argument_parser_type T>
  auto add(std::optional<T>& x, std::string meta) -> argument_parser2&;

  // ------------------------------------------------------------------------

  /// Adds a required named argument.
  template <argument_parser_type T>
  auto add(std::string name, T& x) -> argument_parser2&;

  // ------------------------------------------------------------------------

  /// Adds an optional named argument.
  template <argument_parser_type T>
  auto add(std::string name, std::optional<T>& x) -> argument_parser2&;

  /// Adds an optional named argument.
  auto add(std::string name, std::optional<location>& x) -> argument_parser2&;

  /// Adds an optional named argument.
  auto add(std::string name, bool& x) -> argument_parser2&;

  // ------------------------------------------------------------------------

  auto parse(const operator_factory_plugin::invocation& inv,
             session ctx) -> failure_or<void>;
  auto parse(const ast::function_call& call, session ctx) -> failure_or<void>;
  auto parse(const function_plugin::invocation& inv,
             session ctx) -> failure_or<void>;
  auto parse(const ast::entity& self, std::span<ast::expression const> args,
             session ctx) -> failure_or<void>;

  auto usage() const -> std::string;
  auto docs() const -> std::string;

private:
  enum class kind { op, fn };

  argument_parser2(kind kind, std::string name)
    : kind_{kind}, name_{std::move(name)} {
    // TODO: Remove this temporary hack once we removed TQL1 plugins.
    if (name_.starts_with("tql2.")) {
      name_.erase(0, 5);
    }
  }

  template <class T>
  using setter = std::function<void(T)>;

  template <class... Ts>
  using setter_variant = variant<setter<Ts>...>;

  using any_setter
    = caf::detail::tl_apply_t<argument_parser_full_types, setter_variant>;

  struct positional {
    any_setter set;
    std::string meta;
  };

  struct named {
    std::string name;
    any_setter set;
    bool required = false;
    std::optional<location> found = std::nullopt;
  };

  mutable std::string usage_cache_;
  kind kind_;
  std::vector<positional> positional_;
  std::optional<size_t> first_optional_;
  std::vector<named> named_;
  std::string name_;
};

} // namespace tenzir
