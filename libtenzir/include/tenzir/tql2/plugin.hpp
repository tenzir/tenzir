//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/string_literal.hpp"
#include "tenzir/multi_series.hpp"
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

    auto operator()(const ast::expression& expr) const -> multi_series;

    auto length() const -> int64_t;

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

  struct invocation {
    explicit invocation(const ast::function_call& call) : call{call} {
    }
    ~invocation() = default;
    invocation(const invocation&) = delete;
    invocation(invocation&&) = default;
    auto operator=(const invocation&) -> invocation& = delete;
    auto operator=(invocation&&) -> invocation& = delete;

    const ast::function_call& call;
  };

  virtual auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr>
    = 0;

  virtual auto function_name() const -> std::string;

  virtual auto is_deterministic() const -> bool {
    // TODO: Consider making this pure-virtual or changing the default, as most
    // functions are deterministic.
    return false;
  }
};

class aggregation_instance {
public:
  virtual ~aggregation_instance() = default;

  virtual void update(const table_slice& input, session ctx) = 0;

  virtual auto get() const -> data = 0;

  virtual auto reset() -> void = 0;

  /// Save and restore the state of the aggregation instance. Note that the
  /// restore function should eventually be moved into `aggregation_plugin`, but
  /// we cannot do that yet as quite a few aggregation instances store
  /// `ast::expression`, which is not yet serializable.
  virtual auto save() const -> chunk_ptr = 0;
  virtual auto restore(chunk_ptr chunk, session ctx) -> void = 0;
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

  auto detached() const -> bool override {
    return parser_.detached();
  }

  auto idle_after() const -> duration override {
    return parser_.idle_after();
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto gen = parser_.instantiate(std::move(input), ctrl);
    if (not gen) {
      diagnostic::error("failed to instantiate `{}`", name())
        .emit(ctrl.diagnostics());
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

template <class Loader, detail::string_literal NameOverride = "">
class loader_adapter final : public crtp_operator<loader_adapter<Loader>> {
public:
  loader_adapter() = default;

  explicit loader_adapter(Loader loader) : loader_{std::move(loader)} {
  }

  auto name() const -> std::string override {
    return fmt::format("load_{}", NameOverride.str().empty()
                                    ? Loader{}.name()
                                    : NameOverride.str());
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    co_yield {};
    auto gen = loader_.instantiate(ctrl);
    if (not gen) {
      diagnostic::error("failed to instantiate `{}`", name())
        .emit(ctrl.diagnostics());
      co_return;
    }
    for (auto&& chunk : *gen) {
      co_yield std::move(chunk);
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto internal() const -> bool override {
    return loader_.internal();
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, loader_adapter& x) -> bool {
    return f.apply(x.loader_);
  }

private:
  Loader loader_;
};

// Essentially the tql1 save operator
template <class Saver, detail::string_literal NameOverride = "">
class saver_adapter final : public crtp_operator<saver_adapter<Saver>> {
public:
  saver_adapter() = default;

  explicit saver_adapter(Saver saver) : saver_{std::move(saver)} {
  }

  auto name() const -> std::string override {
    return fmt::format("save_{}", NameOverride.str().empty()
                                    ? Saver{}.name()
                                    : NameOverride.str());
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    co_yield {};
    // TODO: Extend API to allow schema-less make_saver().
    auto new_saver = Saver{saver_}.instantiate(ctrl, std::nullopt);
    if (!new_saver) {
      diagnostic::error(new_saver.error())
        .note("failed to instantiate `{}`", name())
        .emit(ctrl.diagnostics());
      co_return;
    }
    for (auto&& x : input) {
      (*new_saver)(std::move(x));
      co_yield {};
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto internal() const -> bool override {
    return saver_.internal();
  }

  friend auto inspect(auto& f, saver_adapter& x) -> bool {
    return f.apply(x.saver_);
  }

private:
  Saver saver_;
};

// Essentially the tql1 write operator
template <class Writer, detail::string_literal NameOverride = "">
class writer_adapter final : public crtp_operator<writer_adapter<Writer>> {
public:
  writer_adapter() = default;

  explicit writer_adapter(Writer writer) : writer_{std::move(writer)} {
  }

  auto name() const -> std::string override {
    return fmt::format("write_{}", NameOverride.str().empty()
                                     ? Writer{}.name()
                                     : NameOverride.str());
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    co_yield {};
    if (writer_.allows_joining()) {
      auto p = writer_.instantiate(type{}, ctrl);
      if (!p) {
        diagnostic::error(p.error())
          .note("failed to instantiate `{}`", name())
          .emit(ctrl.diagnostics());
        co_return;
      }
      for (auto&& slice : input) {
        for (auto&& chunk : (*p)->process(std::move(slice))) {
          co_yield std::move(chunk);
        }
        if (ctrl.self().getf(caf::abstract_actor::is_shutting_down_flag)) {
          co_return;
        }
      }
      for (auto&& chunk : (*p)->finish()) {
        co_yield std::move(chunk);
      }
    } else {
      auto state
        = std::optional<std::pair<std::unique_ptr<printer_instance>, type>>{};
      for (auto&& slice : input) {
        if (slice.rows() == 0) {
          co_yield {};
          continue;
        }
        if (!state) {
          auto p = writer_.instantiate(slice.schema(), ctrl);
          if (!p) {
            diagnostic::error(p.error())
              .note("failed to initialize `{}`", name())
              .emit(ctrl.diagnostics());
            co_return;
          }
          state = std::pair{std::move(*p), slice.schema()};
        } else if (state->second != slice.schema()) {
          diagnostic::error("`{}` writer does not support heterogeneous "
                            "outputs",
                            writer_.name())
            .note("cannot initialize for schema `{}` after schema `{}`",
                  slice.schema(), state->second)
            .emit(ctrl.diagnostics());
          co_return;
        }
        for (auto&& chunk : state->first->process(std::move(slice))) {
          co_yield std::move(chunk);
        }
        if (ctrl.self().getf(caf::abstract_actor::is_shutting_down_flag)) {
          co_return;
        }
      }
      if (state) {
        for (auto&& chunk : state->first->finish()) {
          co_yield std::move(chunk);
        }
      }
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, writer_adapter& x) -> bool {
    return f.apply(x.writer_);
  }

private:
  Writer writer_;
};
} // namespace tenzir

// TODO: Change this.
#include "tenzir/argument_parser2.hpp"
#include "tenzir/session.hpp"
