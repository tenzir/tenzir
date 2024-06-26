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

template <class T>
struct as_located {
  using type = located<T>;
};

using argument_parser_data_types
  = detail::tl_map_t<caf::detail::tl_filter_not_type_t<data::types, pattern>,
                     as_located>;

using argument_parser_full_types
  = detail::tl_concat_t<argument_parser_data_types,
                        detail::type_list<located<pipeline>, ast::expression>>;

template <class T>
struct is_located : caf::detail::is_specialization<located, T> {};

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
  static auto op(std::string name) -> argument_parser2 {
    return argument_parser2{kind::op, std::move(name)};
  }

  static auto function(std::string name) -> argument_parser2 {
    return argument_parser2{kind::function, std::move(name)};
  }

  static auto method(std::string name) -> argument_parser2 {
    return argument_parser2{kind::method, std::move(name)};
  }

  // ------------------------------------------------------------------------

  template <argument_parser_type T>
  auto add(T& x, std::string meta) -> argument_parser2&;

  template <argument_parser_type T>
  auto add(std::optional<T>& x, std::string meta) -> argument_parser2&;

  // ------------------------------------------------------------------------

  template <argument_parser_type T>
  auto add(std::string name, std::optional<T>& x) -> argument_parser2&;

  auto add(std::string name, std::optional<location>& x) -> argument_parser2&;

  auto add(std::string name, bool& x) -> argument_parser2&;

  // ------------------------------------------------------------------------

  void parse(const operator_factory_plugin::invocation& inv, session ctx);
  void parse(const ast::function_call& call, session ctx);
  void parse(const function_plugin::invocation& inv, session ctx);
  void parse(const ast::entity& self, std::span<ast::expression const> args,
             session ctx);

  auto usage() const -> std::string;
  auto docs() const -> std::string;

private:
  enum class kind { op, function, method };

  argument_parser2(kind kind, std::string name)
    : kind_{kind}, name_{std::move(name)} {
  }

  template <class T>
  using setter = std::function<void(T)>;

  template <class... Ts>
  using setter_variant = variant<setter<Ts>...>;

  struct positional {
    caf::detail::tl_apply_t<argument_parser_full_types, setter_variant> set;
    std::string meta;
  };

  struct named {
    std::string name;
    caf::detail::tl_apply_t<argument_parser_full_types, setter_variant> set;
  };

  mutable std::string usage_cache_;
  kind kind_;
  std::vector<positional> positional_;
  std::optional<size_t> first_optional_;
  std::vector<named> named_;
  std::string name_;
};

} // namespace tenzir
