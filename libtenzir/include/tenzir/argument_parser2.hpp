//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/location.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <caf/detail/is_one_of.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/detail/type_traits.hpp>

#include <functional>

namespace tenzir {

using argument_parser_types
  = detail::type_list<located<std::string>, located<duration>, located<pipeline>,
                      located<bool>, located<uint64_t>, ast::expression>;

template <class T>
struct is_located : caf::detail::is_specialization<located, T> {};

using argument_parser_bare_types
  = detail::tl_map_t<detail::tl_filter_t<argument_parser_types, is_located>,
                     caf::detail::value_type_of>;

static_assert(detail::tl_contains<argument_parser_bare_types, pipeline>::value);

template <class T>
concept argument_parser_type
  = detail::tl_contains<argument_parser_types, T>::value;

template <class T>
concept argument_parser_bare_type
  = detail::tl_contains<argument_parser_bare_types, T>::value;

template <class T>
concept argument_parser_any_type
  = argument_parser_type<T> || argument_parser_bare_type<T>;

class argument_parser2 {
public:
  argument_parser2() = default;

  explicit argument_parser2(std::string docs) : docs_{std::move(docs)} {
  }

  template <argument_parser_any_type T>
  auto add(T& x, std::string meta) -> argument_parser2&;

  template <argument_parser_any_type T>
  auto add(std::optional<T>& x, std::string meta) -> argument_parser2&;

  // ------------------------------------------------------------------------

  auto add(std::string name, std::optional<located<std::string>>& x)
    -> argument_parser2& {
    named_.emplace_back(std::move(name), [&x](located<std::string> y) {
      x = std::move(y);
    });
    return *this;
  }

  auto add(std::string name, std::optional<ast::expression>& x)
    -> argument_parser2& {
    named_.emplace_back(std::move(name), [&x](ast::expression y) {
      x = std::move(y);
    });
    return *this;
  }

  auto add(std::string name, std::optional<location>& x) -> argument_parser2& {
    named_.emplace_back(std::move(name), [&x](located<bool> y) {
      if (y.inner) {
        x = y.source;
      } else {
        x = std::nullopt;
      }
    });
    return *this;
  }

  auto add(std::string name, bool& x) -> argument_parser2& {
    named_.emplace_back(std::move(name), [&x](located<bool> y) {
      x = y.inner;
    });
    return *this;
  }

  void parse(const operator_factory_plugin::invocation& inv, session ctx);

  auto usage() const -> std::string;

private:
  template <class T>
  using setter = std::function<void(T)>;

  template <class... Ts>
  using setter_variant = variant<setter<Ts>...>;

  struct positional {
    caf::detail::tl_apply_t<argument_parser_types, setter_variant> set;
    std::string meta;
  };

  struct named {
    std::string name;
    setter_variant<located<std::string>, ast::expression, located<bool>> set;
  };

  mutable std::string usage_cache_;
  std::vector<positional> positional_;
  std::optional<size_t> first_optional_;
  std::vector<named> named_;
  std::string docs_;
};

} // namespace tenzir
