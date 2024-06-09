//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugin.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"

namespace tenzir {

class operator_factory_plugin : public virtual plugin {
public:
  // Separate from `ast::invocation` in case we want to add things.
  struct invocation {
    ast::entity self;
    std::vector<ast::expression> args;
  };

  virtual auto make(invocation inv, session ctx) const -> operator_ptr = 0;
};

} // namespace tenzir

namespace tenzir::tql2 {

template <class Operator>
class operator_plugin : public virtual operator_factory_plugin,
                        public virtual operator_inspection_plugin<Operator> {};

class function_plugin : public virtual plugin {
public:
  struct named_argument {
    ast::selector selector;
    located<series> series;
  };

  struct positional_argument : located<series> {
    using located::located;
  };

  using argument = variant<positional_argument, named_argument>;

  struct invocation {
    invocation(const ast::function_call& self, int64_t length,
               std::vector<argument> args)
      : self{self}, length{length}, args{std::move(args)} {
    }

    const ast::function_call& self;
    int64_t length;
    std::vector<argument> args;
  };

  virtual auto eval(invocation inv, diagnostic_handler& dh) const -> series = 0;
};

} // namespace tenzir::tql2

namespace tenzir {

class function_argument_parser {
public:
  explicit function_argument_parser(std::string name)
    : docs_{fmt::format("https://docs.tenzir.com/functions/{}", name)} {
  }

  template <type_or_concrete_type... Types>
  auto add(variant<basic_series<Types>...>& x, std::string meta)
    -> function_argument_parser& {
    auto set = [&x, this](located<series> y, diagnostic_handler& dh) {
      auto try_set = [&]<concrete_type Type>(tag<Type>) {
        if (auto cast = try_cast<Type>(y.inner)) {
          x = std::move(*cast);
          return true;
        }
        return false;
      };
      auto success = (try_set(tag_v<Types>) || ...);
      if (not success) {
        auto expected = std::invoke([] {
          auto expected = std::string{};
          auto count = size_t{0};
          (std::invoke(
             [&]<class Type>(tag<Type>) {
               if (count == 0) {
                 expected += fmt::format("{}", type_kind::of<Type>);
               } else if (count < sizeof...(Types) - 1) {
                 expected += fmt::format(", {}", type_kind::of<Type>);
               } else {
                 expected += fmt::format(" or {}", type_kind::of<Type>);
               }
               count += 1;
             },
             tag_v<Types>),
           ...);
          return expected;
        });
        diagnostic::warning("expected {}, but got {}", expected,
                            y.inner.type.kind())
          .primary(y.source)
          .docs(docs_)
          .emit(dh);
        return false;
      }
      return true;
    };
    positional_.emplace_back(set, std::move(meta));
    return *this;
  }

  template <type_or_concrete_type Type>
  auto add(basic_series<Type>& x, std::string meta)
    -> function_argument_parser&;

  template <type_or_concrete_type Type>
  auto add(located<basic_series<Type>>& x, std::string meta)
    -> function_argument_parser&;

  [[nodiscard]] auto
  parse(tql2::function_plugin::invocation& inv, diagnostic_handler& dh) -> bool;

private:
  template <type_or_concrete_type Type>
  static auto try_cast(series x) -> std::optional<basic_series<Type>>;

  struct positional {
    std::function<auto(located<series>, diagnostic_handler& dh)->bool> set;
    std::string meta;
  };

  std::string docs_;
  std::vector<positional> positional_;

  template <std::monostate>
  struct instantiate;
};

class function_use {
public:
  virtual auto eval(const table_slice& input, diagnostic_handler& dh) const
    -> series
    = 0;
};

class aggregation_instance {
public:
  virtual ~aggregation_instance() = default;

  virtual void update(const table_slice& input, session ctx) = 0;

  virtual auto finish() -> data = 0;
};

class aggregation_plugin : public virtual tql2::function_plugin {
public:
  auto eval(invocation inv, diagnostic_handler& dh) const -> series override;

  virtual auto make_aggregation(ast::function_call call, session ctx) const
    -> std::unique_ptr<aggregation_instance>
    = 0;
};

} // namespace tenzir

#include "tenzir/session.hpp"
