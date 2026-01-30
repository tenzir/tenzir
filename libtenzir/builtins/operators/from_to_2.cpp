//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/file.hpp>
#include <tenzir/format_utils.hpp>
#include <tenzir/from_file_base.hpp>
#include <tenzir/glob.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/prepend_token.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql/fwd.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/eval_impl.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/filesystem/api.h>
#include <arrow/util/future.h>
#include <arrow/util/uri.h>
#include <caf/actor_from_state.hpp>
#include <caf/mail_cache.hpp>

#include <ranges>

namespace tenzir::plugins::from {

namespace {

class from_events final : public crtp_operator<from_events> {
public:
  from_events() = default;

  explicit from_events(std::vector<ast::expression> events)
    : events_{std::move(events)} {
  }

  auto name() const -> std::string override {
    return "tql2.from_events";
  }

  auto operator()() const -> generator<table_slice> {
    // We suppress diagnostics here as we already evaluated the expression once
    // as part of the `from` operator. This avoids `from {x: 3 * null}` emitting
    // the same warning twice.
    auto null_dh = null_diagnostic_handler{};
    auto null_sp = session_provider::make(null_dh);
    const auto non_const_eval = [&](const ast::expression& expr) {
      auto value = evaluator{nullptr, null_sp.as_session()}.eval(expr);
      TENZIR_ASSERT(value.length() == 1);
      TENZIR_ASSERT(value.parts().size() == 1);
      return value.part(0);
    };
    for (const auto& expr : events_) {
      auto slice = non_const_eval(expr);
      auto cast = slice.as<record_type>();
      TENZIR_ASSERT(cast);
      auto schema = tenzir::type{"tenzir.from", cast->type};
      co_yield table_slice{arrow::RecordBatch::Make(schema.to_arrow_schema(),
                                                    cast->length(),
                                                    cast->array->fields()),
                           schema};
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, from_events& x) -> bool {
    return f.apply(x.events_);
  }

private:
  std::vector<ast::expression> events_;
};

using from_events_plugin = operator_inspection_plugin<from_events>;

class From final : public Operator<void, table_slice> {
public:
  explicit From(std::vector<ast::expression> events)
    : events_{std::move(events)} {
  }

  auto await_task() const -> Task<std::any> override {
    co_return {};
  }

  auto process_task(std::any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(next_ < events_.size());
    auto sp = session_provider::make(ctx);
    auto value = evaluator{nullptr, sp.as_session()}.eval(events_[next_]);
    TENZIR_ASSERT(value.length() == 1);
    TENZIR_ASSERT(value.parts().size() == 1);
    auto part = std::move(value.part(0));
    auto cast = part.as<record_type>();
    TENZIR_ASSERT(cast);
    auto schema = tenzir::type{"tenzir.from", cast->type};
    auto slice = table_slice{arrow::RecordBatch::Make(schema.to_arrow_schema(),
                                                      cast->length(),
                                                      cast->array->fields()),
                             schema};
    co_await push(std::move(slice));
    next_ += 1;
  }

  auto state() -> OperatorState override {
    if (next_ == events_.size()) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("next", next_);
  }

private:
  size_t next_ = 0;
  std::vector<ast::expression> events_;
};

class from_ir final : public ir::Operator {
public:
  from_ir() = default;

  explicit from_ir(std::vector<ast::expression> events)
    : events_{std::move(events)} {
  }

  auto name() const -> std::string override {
    return "from_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TENZIR_UNUSED(instantiate);
    for (auto& event : events_) {
      TRY(event.substitute(ctx));
    }
    return {};
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<void>());
    return From{std::move(events_)};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    // FIXME
    TENZIR_ASSERT(input.is<void>());
    return tag_v<table_slice>;
  }

  friend auto inspect(auto& f, from_ir& x) -> bool {
    return f.apply(x.events_);
  }

private:
  std::vector<ast::expression> events_;
};

class from_plugin2 final : public virtual operator_factory_plugin,
                           public virtual operator_compiler_plugin {
public:
  constexpr static auto docs
    = "https://docs.tenzir.com/reference/operators/from";
  auto name() const -> std::string override {
    return "tql2.from";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    if (inv.args.empty()) {
      diagnostic::error("expected positional argument `uri|events`")
        .primary(inv.self)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    auto& expr = inv.args[0];
    auto events = std::vector<ast::expression>{};
    TRY(auto value, const_eval(expr, ctx));
    using ret = std::variant<bool, failure_or<operator_ptr>>;
    auto result = match(
      value,
      [&](const record&) -> ret {
        events.push_back(expr);
        return true;
      },
      [&](const std::string& path) -> ret {
        return create_pipeline_from_uri<true>(path, std::move(inv), ctx, docs);
      },
      [&](const auto&) -> ret {
        const auto t = type::infer(value);
        diagnostic::error("expected `string`, or `record`")
          .primary(expr, "got `{}`", t ? t->kind() : type_kind{})
          .docs(docs)
          .emit(ctx);
        return failure::promise();
      });
    if (auto* op = try_as<failure_or<operator_ptr>>(result)) {
      return std::move(*op);
    }
    if (not as<bool>(result)) {
      return failure::promise();
    }
    for (auto& expr : inv.args | std::views::drop(1)) {
      TRY(value, const_eval(expr, ctx));
      result = match(
        value,
        [&](const record&) -> ret {
          events.push_back(expr);
          return true;
        },
        [&](const auto&) -> ret {
          const auto t = type::infer(value);
          diagnostic::error("expected `string`, or `record`")
            .primary(expr, "got `{}`", t ? t->kind() : type_kind{})
            .docs(docs)
            .emit(ctx);
          return failure::promise();
        });
      if (auto* op = try_as<failure_or<operator_ptr>>(result)) {
        return std::move(*op);
      }
    }
    return std::make_unique<from_events>(std::move(events));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> override {
    for (auto& arg : inv.args) {
      TRY(arg.bind(ctx));
    }
    return from_ir{std::move(inv.args)};
  }
};

class to_plugin2 final : public virtual operator_factory_plugin {
public:
  constexpr static auto docs = "https://docs.tenzir.com/reference/operators/to";
  auto name() const -> std::string override {
    return "tql2.to";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    if (inv.args.empty()) {
      diagnostic::error("expected positional argument `uri`")
        .primary(inv.self)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    auto& expr = inv.args[0];
    TRY(auto value, const_eval(expr, ctx));
    return match(
      value,
      [&](std::string& path) -> failure_or<operator_ptr> {
        return create_pipeline_from_uri<false>(path, std::move(inv), ctx, docs);
      },
      [&](auto&) -> failure_or<operator_ptr> {
        auto t = type::infer(value);
        diagnostic::error("expected `string`")
          .primary(inv.args[0], "got `{}`", t ? t->kind() : type_kind{})
          .docs(docs)
          .emit(ctx);
        return failure::promise();
      });
  }
};

class from_file final : public crtp_operator<from_file> {
public:
  from_file() = default;

  explicit from_file(from_file_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "from_file";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto plaintext_url = std::string{};
    co_yield ctrl.resolve_secrets_must_yield({make_secret_request(
      "uri", args_.url, plaintext_url, ctrl.diagnostics())});
    // Spawning the actor detached because some parts of the Arrow filesystem
    // API are blocking.
    auto impl = scope_linked{ctrl.self().spawn<caf::linked + caf::detached>(
      caf::actor_from_state<from_file_state>, args_, std::move(plaintext_url),
      order_,
      std::make_unique<shared_diagnostic_handler>(ctrl.shared_diagnostics()),
      std::string{ctrl.definition()}, ctrl.node(), ctrl.is_hidden(),
      ctrl.metrics_receiver(), ctrl.operator_index(),
      std::string{ctrl.pipeline_id()})};
    while (true) {
      auto result = table_slice{};
      ctrl.self()
        .mail(atom::get_v)
        .request(impl.get(), caf::infinite)
        .then(
          [&](table_slice slice) {
            result = std::move(slice);
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            diagnostic::error(std::move(error)).emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      if (result.rows() == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto optimize(expression const&, event_order order) const
    -> optimize_result override {
    auto copy = std::make_unique<from_file>(*this);
    copy->order_ = order;
    return optimize_result{std::nullopt, event_order::ordered, std::move(copy)};
  }

  friend auto inspect(auto& f, from_file& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_),
                              f.field("order", x.order_));
  }

private:
  from_file_args args_;
  event_order order_{event_order::ordered};
};

class from_file_plugin : public operator_plugin2<from_file> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_file_args{};
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(auto result, args.handle(ctx));
    result.prepend(std::make_unique<from_file>(std::move(args)));
    return std::make_unique<pipeline>(std::move(result));
  }
};

} // namespace

} // namespace tenzir::plugins::from

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_events_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::to_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_file_plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::operator_inspection_plugin<tenzir::from_file_source>);
TENZIR_REGISTER_PLUGIN(
  tenzir::operator_inspection_plugin<tenzir::from_file_sink>);
TENZIR_REGISTER_PLUGIN(tenzir::inspection_plugin<
                       tenzir::ir::Operator, tenzir::plugins::from::from_ir>);
