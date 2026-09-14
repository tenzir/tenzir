//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/string_literal.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/base.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin_api.hpp"

namespace tenzir {

/// Carries the connector, format, and compression properties of an operator.
///
/// The properties drive the URI, extension, and MIME-type based operator
/// selection that `from_file`, `from_http`, and friends perform.
class operator_factory_plugin : public virtual plugin {
public:
  struct load_properties_t {
    /// URI schemes the connector supports
    std::vector<std::string> schemes = {};
    /// A default `load_*` operator to be used if it couldnt be deduced.
    const operator_factory_plugin* default_format = nullptr;
    /// Whether the connector accepts a pipeline as the final argument
    bool accepts_pipeline = false;
    /// Whether to strip the scheme before passing the URI to the transformer
    /// or the operator itself.
    bool strip_scheme = false;
    /// Whether the connector produces/consumes events
    bool events = false;
    /// A function that can be used to transform a URI into ast arguments.
    /// This function may be empty, in which case the URI is just directly
    /// passed as the first argument to the operator.
    /// The location will refer to the URIs location, with the scheme stripped
    /// if requested.
    std::function<failure_or<std::vector<ast::expression>>(located<std::string>,
                                                           diagnostic_handler&)>
      transform_uri = {};
  };

  struct save_properties_t {
    /// URI schemes the connector supports
    std::vector<std::string> schemes = {};
    /// A default `load_*` operator to be used if it couldnt be deduced.
    const operator_factory_plugin* default_format = nullptr;
    /// Whether the connector accepts a pipeline as the final argument
    bool accepts_pipeline = false;
    /// Whether to strip the scheme before passing the URI to the transformer
    /// or the operator itself.
    bool strip_scheme = false;
    /// Whether the connector produces/consumes events
    bool events = false;
    /// A function that can be used to transform a URI into ast arguments.
    /// This function may be empty, in which case the URI is just directly
    /// passed as the first argument to the operator.
    /// The location will refer to the URIs location, with the scheme stripped
    /// if requested.
    std::function<failure_or<std::vector<ast::expression>>(located<std::string>,
                                                           diagnostic_handler&)>
      transform_uri = {};
  };

  virtual auto load_properties() const -> load_properties_t {
    return {};
  }
  virtual auto save_properties() const -> save_properties_t {
    return {};
  }

  struct compression_properties_t {
    std::vector<std::string> extensions = {};
  };
  using decompress_properties_t = compression_properties_t;
  using compress_properties_t = compression_properties_t;
  virtual auto decompress_properties() const -> decompress_properties_t {
    return {};
  }
  virtual auto compress_properties() const -> decompress_properties_t {
    return {};
  }

  struct format_properties_t {
    std::vector<std::string> extensions = {};
    std::vector<std::string> mime_types = {};
  };
  using read_properties_t = format_properties_t;
  using write_properties_t = format_properties_t;
  virtual auto read_properties() const -> read_properties_t {
    return {};
  }
  virtual auto write_properties() const -> write_properties_t {
    return {};
  }
};

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

    auto operator()(const ast::expression& expr) const -> multi_series;

    auto operator()(const ast::lambda_expr& expr,
                    const basic_series<list_type>& input) const -> multi_series;

    auto operator()(const ast::lambda_expr& expr,
                    const basic_series<list_type>& input,
                    int64_t input_offset) const -> multi_series;

    auto length() const -> int64_t;

    auto get_input() const -> Option<table_slice>;

  private:
    void* self_;
  };

  virtual auto run(evaluator eval, session ctx) -> multi_series = 0;

  // TODO: Remove?
  // static auto
  // make(detail::unique_function<auto(evaluator eval, session ctx)->series> f)
  //   -> function_ptr;

  static auto make(
    detail::unique_function<auto(evaluator eval, session ctx)->multi_series> f)
    -> function_ptr;
};

class function_plugin : public virtual plugin {
public:
  using evaluator = function_use::evaluator;

  virtual auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr>
    = 0;

  virtual auto function_name() const -> std::string;

  virtual auto is_deterministic() const -> bool = 0;
};

class aggregation_instance {
public:
  virtual ~aggregation_instance() = default;

  virtual void update(const table_slice& input, session ctx) = 0;

  virtual auto get() const -> data = 0;

  virtual auto reset() -> void = 0;

  /// Save and restore the state of the aggregation instance.
  virtual auto save() const -> chunk_ptr = 0;
  virtual auto restore(chunk_ptr chunk) noexcept -> bool = 0;
};

class aggregation_plugin : public virtual function_plugin {
public:
  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override;

  virtual auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>>
    = 0;

  /// Returns the output type for regular list calls when it can be inferred
  /// from the list element type.
  virtual auto list_call_result_type(type const&) const -> Option<type> {
    return None{};
  }
};

// Forward declarations for operator_compiler_plugin.
namespace ir {
class Operator;
struct CompileResult;
} // namespace ir
class compile_ctx;
template <class T>
class Box;

/// Plugin for transforming the AST of an operator invocation to its IR.
class operator_compiler_plugin : public virtual plugin {
public:
  /// Return the IR compile result for the given AST invocation.
  ///
  /// Note that any `let` bindings in the arguments are not bound yet. This
  /// means that the implementation must call `expr.bind(ctx)` itself. The
  /// reason for that is that pipeline expressions can not be bound because the
  /// operator itself can introduce new bindings. Thus, we cannot bind inside
  /// pipeline expressions. For consistency, we decided to not bind anything.
  virtual auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult>
    = 0;

  /// Return the name of the operator, including `::` for modules.
  ///
  /// By default, this returns the name of the plugin.
  virtual auto operator_name() const -> std::string;
};

} // namespace tenzir

// TODO: Change this.
#include "tenzir/argument_parser2.hpp"
#include "tenzir/session.hpp"
