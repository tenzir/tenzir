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

#include <caf/detail/concepts.hpp>
#include <caf/detail/type_list.hpp>

#include <functional>

namespace tenzir {

namespace detail {

template <class T>
struct value_type_of {
  using type = typename T::value_type;
};

} // namespace detail

using argument_parser_data_types
  = detail::tl_map_t<detail::tl_filter_not_type_t<data::types, pattern>,
                     as_located>;

using argument_parser_full_types = detail::tl_concat_t<
  argument_parser_data_types,
  detail::type_list<located<pipeline>, ast::expression, ast::field_path,
                    ast::lambda_expr, located<data>>>;

using argument_parser_bare_types
  = detail::tl_map_t<detail::tl_filter_t<argument_parser_full_types, is_located>,
                     detail::value_type_of>;

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

  static auto context(std::string name) -> argument_parser2 {
    return argument_parser2{
      kind::op, fmt::format("context::create_{}",
                            detail::replace_all(std::move(name), "-", "_"))};
  }

  // ------------------------------------------------------------------------

  /// Adds a required positional argument.
  template <argument_parser_type T>
  auto positional(std::string name, T& x, std::string type = maybe_default<T>)
    -> argument_parser2&;

  /// Adds an optional positional argument.
  template <argument_parser_type T>
  auto positional(std::string name, std::optional<T>& x,
                  std::string type = maybe_default<T>) -> argument_parser2&;

  // ------------------------------------------------------------------------

  /// Adds a required named argument.
  template <argument_parser_type T>
  auto named(std::string name, T& x, std::string type = maybe_default<T>)
    -> argument_parser2&;

  /// Adds an optional named argument. Use this is "Not Given" is a case you
  /// need to handle.
  template <argument_parser_type T>
  auto named(std::string name, std::optional<T>& x,
             std::string type = maybe_default<T>) -> argument_parser2&;

  /// Adds an optional named argument. Use this if you have an object with a
  /// default value.
  template <argument_parser_type T>
  auto
  named_optional(std::string name, T& x, std::string type = maybe_default<T>)
    -> argument_parser2&;

  /// Adds an optional named argument.
  auto named(std::string name, std::optional<location>& x,
             std::string type = "") -> argument_parser2&;

  /// Adds an optional named argument.
  auto named(std::string name, bool& x, std::string type = "")
    -> argument_parser2&;

  // ------------------------------------------------------------------------

  auto parse(const operator_factory_plugin::invocation& inv, session ctx)
    -> failure_or<void>;
  auto parse(const ast::function_call& call, session ctx) -> failure_or<void>;
  auto parse(const function_plugin::invocation& inv, session ctx)
    -> failure_or<void>;
  auto parse(const ast::entity& self, std::span<ast::expression const> args,
             session ctx) -> failure_or<void>;

  auto usage() const -> std::string;
  auto docs() const -> std::string;

private:
  /// For some types, we do not want to implicit default to a generic string. If
  /// your code fails to compile because of this constraint, add a third
  /// parameter which described the argument "type".
  ///
  /// Records could also be disallowed here, but unlike lists, it seems like we
  /// use "record" anyway and do not specify a more concrete type.
  template <class T>
    requires(not concepts::one_of<T, ast::expression, list, located<list>>)
  static constexpr char const* maybe_default = "";

  enum class kind { op, fn };

  argument_parser2(kind kind, std::string name)
    : kind_{kind}, name_{std::move(name)} {
    // TODO: Remove this temporary hack once we removed TQL1 plugins.
    if (name_.starts_with("tql2.")) {
      name_.erase(0, 5);
    }
  }

  template <class T>
  static auto make_setter(T& x) -> auto;

  template <class T>
  using setter = std::function<void(T)>;

  template <class... Ts>
  using setter_variant = variant<setter<Ts>...>;

  using any_setter
    = detail::tl_apply_t<argument_parser_full_types, setter_variant>;

  struct positional_t {
    positional_t(std::string name, std::string type, any_setter set)
      : name{std::move(name)}, type{std::move(type)}, set{std::move(set)} {
    }

    std::string name;
    std::string type;
    any_setter set;
  };

  struct named_t {
    named_t(std::string_view name, std::string type, any_setter set,
            bool required)
      : type{std::move(type)}, set{std::move(set)}, required{required} {
      for (auto part : detail::split(name, "|")) {
        names.emplace_back(part);
      }
    }

    std::vector<std::string> names;
    std::string type;
    any_setter set;
    bool required = false;
    std::optional<location> found = std::nullopt;
  };

  mutable std::string usage_cache_;
  kind kind_;
  std::vector<positional_t> positional_;
  std::optional<size_t> first_optional_;
  std::vector<named_t> named_;
  std::string name_;
};

struct argument_info {
  argument_info(std::string_view name, const located<std::string>& value)
    : name{name}, value{value.inner}, loc{value.source} {
  }
  argument_info(std::string_view name, std::string_view value)
    : name{name}, value{value} {
  }
  argument_info(std::string_view name,
                const std::optional<located<std::string>>& value)
    : name{name},
      value{value ? value->inner : std::string{}},
      loc{value ? value->source : location::unknown} {
  }
  std::string_view name;
  std::string_view value;
  location loc = location::unknown;
};

/// Ensures that none of the given string values is a substring of another,
/// ignoring empty strings.
auto check_no_substrings(diagnostic_handler& dh,
                         std::vector<argument_info> values) -> failure_or<void>;

/// Ensures that the argument is not empty.
auto check_non_empty(std::string_view name, const located<std::string>& v,
                     diagnostic_handler& dh) -> failure_or<void>;

} // namespace tenzir
