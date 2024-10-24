//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/string_literal.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

class operator_factory_plugin : public virtual plugin {
public:
  // Separate from `ast::invocation` in case we want to add things.
  struct invocation {
    ast::entity self;
    std::vector<ast::expression> args;
  };

  virtual auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr>
    = 0;

  /// Returns the URI schemes from which the operator can load (e.g., `http`).
  virtual auto load_schemes() const -> std::vector<std::string> {
    return {};
  }

  /// Returns the URI schemes to which the operator can save (e.g., `s3`).
  virtual auto save_schemes() const -> std::vector<std::string> {
    return {};
  }

  /// Returns the file extensions which the operator can parse (e.g. `yaml`).
  virtual auto read_extensions() const -> std::vector<std::string> {
    return {};
  }
  // whether this operator accepts a sub-pipeline. This is used in the tql2
  // `from` implementation.
  virtual auto load_accepts_pipeline() const -> bool {
    return false;
  }
};

template <class Operator>
class operator_plugin2 : public virtual operator_factory_plugin,
                         public virtual operator_inspection_plugin<Operator> {};

class function_use;

using function_ptr = std::unique_ptr<function_use>;

class function_use {
public:
  virtual ~function_use() = default;

  // TODO: Improve this.
  class evaluator {
  public:
    explicit evaluator(void* self) : self_{self} {
    }

    auto operator()(const ast::expression& expr) const -> series;

    auto length() const -> int64_t;

  private:
    void* self_;
  };

  virtual auto run(evaluator eval, session ctx) const -> series = 0;

  static auto make(std::function<auto(evaluator eval, session ctx)->series> f)
    -> function_ptr;
};
class function_plugin : public virtual plugin {
public:
  using evaluator = function_use::evaluator;

  struct invocation {
    explicit invocation(const ast::function_call& call) : call{call} {
    }
    invocation(const invocation&) = delete;
    invocation(invocation&&) = delete;
    auto operator=(const invocation&) -> invocation& = delete;
    auto operator=(invocation&&) -> invocation& = delete;

    const ast::function_call& call;
  };

  virtual auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr>
    = 0;

  virtual auto function_name() const -> std::string;
};

class method_plugin : public virtual function_plugin {};

class aggregation_instance {
public:
  virtual ~aggregation_instance() = default;

  virtual void update(const table_slice& input, session ctx) = 0;

  virtual auto finish() -> data = 0;
};

class aggregation_plugin : public virtual function_plugin {
public:
  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override;

  virtual auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>>
    = 0;
};

/// This adapter transforms a legacy parser object to an operator.
///
/// Should be deleted once the transition is done.
template <class Parser, detail::string_literal NameOverride = "">
class parser_adapter final : public crtp_operator<parser_adapter<Parser>> {
public:
  parser_adapter() = default;

  explicit parser_adapter(Parser parser) : parser_{std::move(parser)} {
  }

  auto name() const -> std::string override {
    return fmt::format("read_{}", NameOverride.str().empty()
                                    ? Parser{}.name()
                                    : NameOverride.str());
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto gen = parser_.instantiate(std::move(input), ctrl);
    if (not gen) {
      co_return;
    }
    for (auto&& slice : *gen) {
      co_yield std::move(slice);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter);
    // TODO: Function should be const.
    auto parser = parser_;
    auto replacement = parser.optimize(order);
    if (not replacement) {
      return optimize_result{
        std::nullopt,
        event_order::ordered,
        std::make_unique<parser_adapter>(std::move(parser)),
      };
    }
    // TODO: This is a hack.
    auto cast = dynamic_cast<Parser*>(replacement.get());
    TENZIR_ASSERT(cast);
    return optimize_result{
      std::nullopt,
      event_order::ordered,
      std::make_unique<parser_adapter>(std::move(*cast)),
    };
  }

  friend auto inspect(auto& f, parser_adapter& x) -> bool {
    return f.apply(x.parser_);
  }

private:
  Parser parser_;
};

} // namespace tenzir

// TODO: Change this.
#include "tenzir/argument_parser2.hpp"
#include "tenzir/session.hpp"
